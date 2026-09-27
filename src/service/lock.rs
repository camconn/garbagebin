//! Object Lock: bucket configuration, per-version retention and legal hold, and
//! the rules deciding when a version may be deleted or its retention changed.

use std::time::{Duration, SystemTime};

use s3s::dto::{
    DefaultRetention, GetObjectLegalHoldInput, GetObjectLegalHoldOutput,
    GetObjectLockConfigurationInput, GetObjectLockConfigurationOutput, GetObjectRetentionInput,
    GetObjectRetentionOutput, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode,
    ObjectLockRule, PutObjectLegalHoldInput, PutObjectLegalHoldOutput,
    PutObjectLockConfigurationInput, PutObjectLockConfigurationOutput, PutObjectRetentionInput,
    PutObjectRetentionOutput, Timestamp,
};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

use super::{GarbageBin, error, invalid_argument, read_err, storage_err, ts, version_param};
use crate::storage::{
    BucketInfo, LockMode, ObjectLock, ObjectMeta, Retention, RetentionPeriod, Storage,
    StorageError, Versioning,
};

const DAY: u64 = 24 * 60 * 60;
const MAX_DAYS: i32 = 36_500;
const MAX_YEARS: i32 = 100;

/// Passes a service error through a storage closure.
pub(super) fn refused(e: S3Error) -> StorageError {
    StorageError::Refused(Box::new(e))
}

fn access_denied() -> S3Error {
    error(
        S3ErrorCode::AccessDenied,
        "Access Denied because object protected by object lock.",
    )
}

fn malformed_xml() -> S3Error {
    error(
        S3ErrorCode::MalformedXML,
        "The XML you provided was not well-formed or did not validate against our published schema",
    )
}

fn not_lock_enabled() -> S3Error {
    error(
        S3ErrorCode::InvalidRequest,
        "Bucket is missing Object Lock Configuration",
    )
}

fn in_past() -> S3Error {
    invalid_argument("The retain until date must be in the future!")
}

fn parse_mode(s: &str) -> Option<LockMode> {
    match s {
        ObjectLockRetentionMode::GOVERNANCE => Some(LockMode::Governance),
        ObjectLockRetentionMode::COMPLIANCE => Some(LockMode::Compliance),
        _ => None,
    }
}

fn mode_str(m: LockMode) -> &'static str {
    match m {
        LockMode::Governance => ObjectLockRetentionMode::GOVERNANCE,
        LockMode::Compliance => ObjectLockRetentionMode::COMPLIANCE,
    }
}

fn hold_str(on: bool) -> &'static str {
    if on {
        ObjectLockLegalHoldStatus::ON
    } else {
        ObjectLockLegalHoldStatus::OFF
    }
}

fn parse_hold(s: &str) -> Option<bool> {
    match s {
        ObjectLockLegalHoldStatus::ON => Some(true),
        ObjectLockLegalHoldStatus::OFF => Some(false),
        _ => None,
    }
}

fn system_time(t: &Timestamp) -> SystemTime {
    time::OffsetDateTime::from(t.clone()).into()
}

fn require_lock(info: &BucketInfo) -> S3Result<ObjectLock> {
    info.object_lock.ok_or_else(not_lock_enabled)
}

/// Lock fields a new version gets from `x-amz-object-lock-*` headers.
/// Retention is `None` if the headers give none; see [`default_retention`].
pub(super) fn from_headers(
    info: &BucketInfo,
    mode: Option<&ObjectLockMode>,
    until: Option<&Timestamp>,
    hold: Option<&ObjectLockLegalHoldStatus>,
    now: SystemTime,
) -> S3Result<(Option<Retention>, Option<bool>)> {
    if mode.is_none() && until.is_none() && hold.is_none() {
        return Ok((None, None));
    }
    require_lock(info)?;
    let retention = match (mode, until) {
        (None, None) => None,
        (Some(m), Some(u)) => {
            let mode = parse_mode(m.as_str())
                .ok_or_else(|| invalid_argument("Unknown wormMode directive."))?;
            let until = system_time(u);
            if until <= now {
                return Err(in_past());
            }
            Some(Retention { mode, until })
        }
        _ => {
            return Err(invalid_argument(
                "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied",
            ));
        }
    };
    let hold = hold
        .map(|h| {
            parse_hold(h.as_str())
                .ok_or_else(|| invalid_argument("Legal Hold must be either of 'ON' or 'OFF'"))
        })
        .transpose()?;
    Ok((retention, hold))
}

/// Retention from the bucket's default rule for a version created at `now`.
pub(super) fn default_retention(info: &BucketInfo, now: SystemTime) -> Option<Retention> {
    let (mode, period) = info.object_lock?.default_retention?;
    let days = match period {
        RetentionPeriod::Days(d) => u64::from(d),
        RetentionPeriod::Years(y) => u64::from(y) * 365,
    };
    Some(Retention {
        mode,
        until: now + Duration::from_secs(days * DAY),
    })
}

/// Whether version `meta` may be permanently deleted at `now`.
pub(super) fn check_delete(meta: &ObjectMeta, now: SystemTime, bypass: bool) -> S3Result<()> {
    if meta.legal_hold == Some(true) {
        return Err(access_denied());
    }
    match meta.retention {
        Some(r) if r.until > now && !(r.mode == LockMode::Governance && bypass) => {
            Err(access_denied())
        }
        _ => Ok(()),
    }
}

/// Whether retention `old` may become `new` at `now`. Unexpired COMPLIANCE can only be
/// extended; unexpired GOVERNANCE can only be extended unless `bypass`.
fn check_retention_change(
    old: Option<Retention>,
    new: Option<Retention>,
    now: SystemTime,
    bypass: bool,
) -> S3Result<()> {
    let Some(old) = old.filter(|r| r.until > now) else {
        return Ok(());
    };
    let extends = new.is_some_and(|n| n.mode == old.mode && n.until >= old.until);
    if extends || (old.mode == LockMode::Governance && bypass) {
        Ok(())
    } else {
        Err(access_denied())
    }
}

pub async fn put_object_lock_configuration<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<PutObjectLockConfigurationInput>,
) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
    let i = req.input;
    let cfg = i.object_lock_configuration.ok_or_else(malformed_xml)?;
    if cfg.object_lock_enabled.as_ref().map(|e| e.as_str()) != Some(ObjectLockEnabled::ENABLED) {
        return Err(malformed_xml());
    }
    let default_retention = match cfg.rule {
        None => None,
        Some(rule) => {
            let d = rule.default_retention.ok_or_else(malformed_xml)?;
            let mode = d
                .mode
                .as_ref()
                .and_then(|m| parse_mode(m.as_str()))
                .ok_or_else(malformed_xml)?;
            Some((mode, period(d.days, d.years)?))
        }
    };
    let f = Box::new(move |b: &mut BucketInfo| {
        if b.object_lock.is_none() && b.versioning != Some(Versioning::Enabled) {
            return Err(refused(error(
                S3ErrorCode::InvalidBucketState,
                "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration",
            )));
        }
        b.object_lock = Some(ObjectLock { default_retention });
        Ok(())
    });
    gb.storage
        .update_bucket(&i.bucket, f)
        .await
        .map_err(storage_err)?;
    Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
}

fn period(days: Option<i32>, years: Option<i32>) -> S3Result<RetentionPeriod> {
    let invalid = |msg: &str| {
        let mut e = error(S3ErrorCode::Custom("InvalidRetentionPeriod".into()), msg);
        e.set_status_code(http::StatusCode::BAD_REQUEST);
        e
    };
    let (n, max) = match (days, years) {
        (Some(d), None) => (d, MAX_DAYS),
        (None, Some(y)) => (y, MAX_YEARS),
        _ => return Err(malformed_xml()),
    };
    if n <= 0 {
        return Err(invalid(
            "Default retention period must be a positive integer value.",
        ));
    }
    if n > max {
        return Err(invalid("Default retention period is too large."));
    }
    let n = n as u32;
    Ok(if days.is_some() {
        RetentionPeriod::Days(n)
    } else {
        RetentionPeriod::Years(n)
    })
}

pub async fn get_object_lock_configuration<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<GetObjectLockConfigurationInput>,
) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
    let info = gb.require_bucket(&req.input.bucket).await?;
    let lock = info.object_lock.ok_or_else(|| {
        error(
            S3ErrorCode::ObjectLockConfigurationNotFoundError,
            "Object Lock configuration does not exist for this bucket",
        )
    })?;
    let rule = lock.default_retention.map(|(mode, period)| {
        let (days, years) = match period {
            RetentionPeriod::Days(d) => (Some(d as i32), None),
            RetentionPeriod::Years(y) => (None, Some(y as i32)),
        };
        ObjectLockRule {
            default_retention: Some(DefaultRetention {
                mode: Some(ObjectLockRetentionMode::from_static(mode_str(mode))),
                days,
                years,
            }),
        }
    });
    Ok(S3Response::new(GetObjectLockConfigurationOutput {
        object_lock_configuration: Some(ObjectLockConfiguration {
            object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
            rule,
        }),
    }))
}

pub async fn put_object_retention<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<PutObjectRetentionInput>,
) -> S3Result<S3Response<PutObjectRetentionOutput>> {
    let i = req.input;
    let version = version_param(i.version_id.as_ref())?;
    require_lock(&gb.require_bucket(&i.bucket).await?)?;
    let now = gb.clock.now();
    let r = i.retention.unwrap_or_default();
    let new = match (r.mode, r.retain_until_date) {
        (None, None) => None,
        (Some(m), Some(u)) => {
            let mode = parse_mode(m.as_str()).ok_or_else(malformed_xml)?;
            let until = system_time(&u);
            if until <= now {
                return Err(in_past());
            }
            Some(Retention { mode, until })
        }
        _ => return Err(malformed_xml()),
    };
    let bypass = i.bypass_governance_retention.unwrap_or(false);
    let f = Box::new(move |m: &mut ObjectMeta| {
        check_retention_change(m.retention, new, now, bypass).map_err(refused)?;
        m.retention = new;
        Ok(())
    });
    gb.storage
        .update_object(&i.bucket, &i.key, version, f)
        .await
        .map_err(|e| read_err(e, version.is_some()))?;
    Ok(S3Response::new(PutObjectRetentionOutput::default()))
}

pub async fn get_object_retention<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<GetObjectRetentionInput>,
) -> S3Result<S3Response<GetObjectRetentionOutput>> {
    let i = req.input;
    let meta = lock_enabled_version(gb, &i.bucket, &i.key, i.version_id.as_ref()).await?;
    let r = meta.retention.ok_or_else(no_lock_config)?;
    Ok(S3Response::new(GetObjectRetentionOutput {
        retention: Some(ObjectLockRetention {
            mode: Some(ObjectLockRetentionMode::from_static(mode_str(r.mode))),
            retain_until_date: Some(ts(r.until)),
        }),
    }))
}

pub async fn put_object_legal_hold<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<PutObjectLegalHoldInput>,
) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
    let i = req.input;
    let version = version_param(i.version_id.as_ref())?;
    require_lock(&gb.require_bucket(&i.bucket).await?)?;
    let on = i
        .legal_hold
        .and_then(|h| h.status)
        .and_then(|s| parse_hold(s.as_str()))
        .ok_or_else(malformed_xml)?;
    let f = Box::new(move |m: &mut ObjectMeta| {
        m.legal_hold = Some(on);
        Ok(())
    });
    gb.storage
        .update_object(&i.bucket, &i.key, version, f)
        .await
        .map_err(|e| read_err(e, version.is_some()))?;
    Ok(S3Response::new(PutObjectLegalHoldOutput::default()))
}

pub async fn get_object_legal_hold<S: Storage>(
    gb: &GarbageBin<S>,
    req: S3Request<GetObjectLegalHoldInput>,
) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
    let i = req.input;
    let meta = lock_enabled_version(gb, &i.bucket, &i.key, i.version_id.as_ref()).await?;
    let on = meta.legal_hold.ok_or_else(no_lock_config)?;
    Ok(S3Response::new(GetObjectLegalHoldOutput {
        legal_hold: Some(ObjectLockLegalHold {
            status: Some(ObjectLockLegalHoldStatus::from_static(hold_str(on))),
        }),
    }))
}

fn no_lock_config() -> S3Error {
    error(
        S3ErrorCode::NoSuchObjectLockConfiguration,
        "The specified object does not have a ObjectLock configuration",
    )
}

/// The addressed version, in a bucket with Object Lock enabled.
async fn lock_enabled_version<S: Storage>(
    gb: &GarbageBin<S>,
    bucket: &str,
    key: &str,
    version: Option<&String>,
) -> S3Result<ObjectMeta> {
    let version = version_param(version)?;
    require_lock(&gb.require_bucket(bucket).await?)?;
    gb.storage
        .head_object(bucket, key, version)
        .await
        .map_err(|e| read_err(e, version.is_some()))
}

/// `x-amz-object-lock-*` response headers of a version.
pub(super) fn headers(
    m: &ObjectMeta,
) -> (
    Option<ObjectLockMode>,
    Option<Timestamp>,
    Option<ObjectLockLegalHoldStatus>,
) {
    (
        m.retention
            .map(|r| ObjectLockMode::from_static(mode_str(r.mode))),
        m.retention.map(|r| ts(r.until)),
        m.legal_hold
            .map(|on| ObjectLockLegalHoldStatus::from_static(hold_str(on))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn r(mode: LockMode, until: u64) -> Option<Retention> {
        Some(Retention {
            mode,
            until: at(until),
        })
    }

    #[test]
    fn retention_changes() {
        use LockMode::{Compliance as C, Governance as G};
        let ok = |old, new, bypass| check_retention_change(old, new, at(100), bypass).is_ok();
        // Nothing, or expired: anything goes.
        assert!(ok(None, r(C, 200), false));
        assert!(ok(r(C, 100), None, false));
        // Extending in the same mode is always allowed.
        assert!(ok(r(C, 200), r(C, 300), false));
        assert!(ok(r(G, 200), r(G, 200), false));
        // Compliance: no shortening, removal or mode change, even with bypass.
        assert!(!ok(r(C, 200), r(C, 150), true));
        assert!(!ok(r(C, 200), None, true));
        assert!(!ok(r(C, 200), r(G, 300), true));
        // Governance: only with bypass.
        assert!(!ok(r(G, 200), r(G, 150), false));
        assert!(!ok(r(G, 200), None, false));
        assert!(!ok(r(G, 200), r(C, 300), false));
        assert!(ok(r(G, 200), r(G, 150), true));
        assert!(ok(r(G, 200), None, true));
        assert!(ok(r(G, 200), r(C, 200), true));
    }

    #[test]
    fn deletes() {
        let mut m = ObjectMeta::new(0, String::new());
        let ok = |m: &ObjectMeta, bypass| check_delete(m, at(100), bypass).is_ok();
        assert!(ok(&m, false));
        m.legal_hold = Some(false);
        assert!(ok(&m, false));
        m.retention = r(LockMode::Governance, 200);
        assert!(!ok(&m, false));
        assert!(ok(&m, true));
        m.retention = r(LockMode::Compliance, 200);
        assert!(!ok(&m, true));
        m.retention = r(LockMode::Compliance, 100);
        assert!(ok(&m, false));
        m.legal_hold = Some(true);
        assert!(!ok(&m, true));
    }

    #[test]
    fn periods() {
        assert_eq!(period(Some(1), None).unwrap(), RetentionPeriod::Days(1));
        assert_eq!(
            period(None, Some(100)).unwrap(),
            RetentionPeriod::Years(100)
        );
        for (d, y) in [(Some(1), Some(1)), (None, None)] {
            assert_eq!(*period(d, y).unwrap_err().code(), S3ErrorCode::MalformedXML);
        }
        for (d, y) in [(Some(0), None), (None, Some(-1)), (Some(36_501), None)] {
            assert_eq!(
                period(d, y).unwrap_err().code().as_str(),
                "InvalidRetentionPeriod"
            );
        }
    }
}
