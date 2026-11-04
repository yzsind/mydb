//! Shared AES key-wrap primitive for recoverable credential envelopes.
use crate::types::QueryError;
pub(crate) fn wrap(key: &[u8], input: &[u8], encrypt: bool) -> Result<Vec<u8>, QueryError> {
    use openssl::{
        cipher::Cipher,
        cipher_ctx::{CipherCtx, CipherCtxFlags},
    };
    let fail =
        || crate::error::catalog::ER_UNKNOWN_ERROR.error("Credential key authentication failed");
    let mut ctx = CipherCtx::new().map_err(|_| fail())?;
    ctx.set_flags(CipherCtxFlags::FLAG_WRAP_ALLOW);
    if encrypt {
        ctx.encrypt_init(Some(Cipher::aes_256_wrap_pad()), Some(key), None)
    } else {
        ctx.decrypt_init(Some(Cipher::aes_256_wrap_pad()), Some(key), None)
    }
    .map_err(|_| fail())?;
    let mut out = zeroize::Zeroizing::new(Vec::new());
    ctx.cipher_update_vec(input, &mut out).map_err(|_| fail())?;
    ctx.cipher_final_vec(&mut out).map_err(|_| fail())?;
    Ok(std::mem::take(&mut *out))
}
