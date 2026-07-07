use crate::error::Error;

pub fn read_u8(payload: &mut &[u8]) -> Result<u8, Error> {
    if payload.is_empty() {
        return Err(Error::UnexpectedEof);
    }
    let v = payload[0];
    *payload = &payload[1..];
    Ok(v)
}

pub fn read_u16_le(payload: &mut &[u8]) -> Result<u16, Error> {
    if payload.len() < 2 {
        return Err(Error::UnexpectedEof);
    }
    let v = u16::from_le_bytes([payload[0], payload[1]]);
    *payload = &payload[2..];
    Ok(v)
}

pub fn read_u24_le(payload: &mut &[u8]) -> Result<u32, Error> {
    if payload.len() < 3 {
        return Err(Error::UnexpectedEof);
    }
    let v = u32::from_le_bytes([payload[0], payload[1], payload[2], 0]);
    *payload = &payload[3..];
    Ok(v)
}

pub fn read_u32_le(payload: &mut &[u8]) -> Result<u32, Error> {
    if payload.len() < 4 {
        return Err(Error::UnexpectedEof);
    }
    let v = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    *payload = &payload[4..];
    Ok(v)
}

pub fn read_u64_le(payload: &mut &[u8]) -> Result<u64, Error> {
    if payload.len() < 8 {
        return Err(Error::UnexpectedEof);
    }
    let v = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    *payload = &payload[8..];
    Ok(v)
}

pub fn read_i8(payload: &mut &[u8]) -> Result<i8, Error> {
    Ok(read_u8(payload)? as i8)
}

pub fn read_i16_le(payload: &mut &[u8]) -> Result<i16, Error> {
    Ok(read_u16_le(payload)? as i16)
}

pub fn read_i24_le(payload: &mut &[u8]) -> Result<i32, Error> {
    let v = read_u24_le(payload)?;
    let sign = v & 0x800000;
    if sign != 0 {
        Ok((v | 0xFF000000) as i32)
    } else {
        Ok(v as i32)
    }
}

pub fn read_i32_le(payload: &mut &[u8]) -> Result<i32, Error> {
    Ok(read_u32_le(payload)? as i32)
}

pub fn read_i64_le(payload: &mut &[u8]) -> Result<i64, Error> {
    Ok(read_u64_le(payload)? as i64)
}

pub fn read_f32_le(payload: &mut &[u8]) -> Result<f32, Error> {
    Ok(f32::from_le_bytes(read_u32_le(payload)?.to_le_bytes()))
}

pub fn read_f64_le(payload: &mut &[u8]) -> Result<f64, Error> {
    Ok(f64::from_le_bytes(read_u64_le(payload)?.to_le_bytes()))
}

pub fn read_bytes(payload: &mut &[u8], len: usize) -> Result<Vec<u8>, Error> {
    if payload.len() < len {
        return Err(Error::UnexpectedEof);
    }
    let v = payload[..len].to_vec();
    *payload = &payload[len..];
    Ok(v)
}

pub fn read_lenenc_int(payload: &mut &[u8]) -> Result<u64, Error> {
    let first = read_u8(payload)?;
    match first {
        0xfb => Ok(0),
        0xfc => Ok(read_u16_le(payload)? as u64),
        0xfd => Ok(read_u24_le(payload)? as u64),
        0xfe => Ok(read_u64_le(payload)?),
        n => Ok(n as u64),
    }
}

pub fn read_cstring(payload: &mut &[u8]) -> Result<String, Error> {
    let pos = payload
        .iter()
        .position(|&b| b == 0)
        .ok_or(Error::UnexpectedEof)?;
    let s = String::from_utf8_lossy(&payload[..pos]).into_owned();
    *payload = &payload[pos + 1..];
    Ok(s)
}

pub fn read_uint_be(payload: &mut &[u8], nbytes: usize) -> Result<u64, Error> {
    if payload.len() < nbytes {
        return Err(Error::UnexpectedEof);
    }
    let mut v = 0u64;
    for &b in &payload[..nbytes] {
        v = (v << 8) | u64::from(b);
    }
    *payload = &payload[nbytes..];
    Ok(v)
}

pub fn read_bitmap(payload: &mut &[u8], nbits: usize) -> Result<Vec<u8>, Error> {
    let nbytes = nbits.div_ceil(8);
    read_bytes(payload, nbytes)
}

pub fn read_u6_le(payload: &mut &[u8]) -> Result<u64, Error> {
    if payload.len() < 6 {
        return Err(Error::UnexpectedEof);
    }
    let mut buf = [0u8; 8];
    buf[..6].copy_from_slice(&payload[..6]);
    *payload = &payload[6..];
    Ok(u64::from_le_bytes(buf))
}
