use odbc_sys::{
    AttrOdbcVersion, CDataType, Dbc, Env, EnvironmentAttribute, FreeStmtOption, HandleType, Obj,
    ParamType, SQLAllocHandle, SQLBindParameter, SQLConnect, SQLDisconnect, SQLExecDirect,
    SQLExecute, SQLFetch, SQLFreeHandle, SQLFreeStmt, SQLGetData, SQLGetDiagRec, SQLPrepare,
    SQLSetEnvAttr, SqlDataType, SqlReturn, Stmt,
};
use std::convert::TryInto;
use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;

#[derive(Debug)]
pub struct DiagnosticRecord {
    pub native_error: i32,
    pub message: String,
}

#[derive(Debug)]
pub enum Error {
    NoDiagnositics,
    Diagnostics(DiagnosticRecord),
}

pub type Result<T> = std::result::Result<T, Error>;

unsafe fn get_diag_rec(handle_type: HandleType, handle: *mut Obj) -> DiagnosticRecord {
    let mut text_length = 0;
    let mut state = [0; 6];
    let mut native_error = 0;
    let mut message_bytes = vec![0; 1024];

    match SQLGetDiagRec(
        handle_type,
        handle,
        1,
        state.as_mut_ptr(),
        &mut native_error,
        message_bytes.as_mut_ptr(),
        message_bytes.len() as i16,
        &mut text_length,
    ) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => {
            let message = CString::from_vec_with_nul_unchecked(message_bytes)
                .into_string()
                .unwrap();

            DiagnosticRecord {
                native_error,
                message,
            }
        }
        SqlReturn(code) => panic!("SQLGetDiagRec returned error code ({})", code),
    }
}

unsafe fn alloc_handle(handle_type: HandleType, input_handle: *mut Obj) -> Result<*mut Obj> {
    let mut handle = ptr::null_mut();
    match SQLAllocHandle(handle_type, input_handle, &mut handle) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(handle),
        _ => Err(Error::NoDiagnositics),
    }
}

unsafe fn free_handle(handle_type: HandleType, handle: *mut Obj) -> Result<()> {
    match SQLFreeHandle(handle_type, handle) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(handle_type, handle))),
    }
}

pub unsafe fn alloc_env() -> Result<*mut Env> {
    let env = alloc_handle(HandleType::Env, ptr::null_mut())? as *mut Env;

    match SQLSetEnvAttr(
        env,
        EnvironmentAttribute::OdbcVersion,
        AttrOdbcVersion::Odbc3.into(),
        0,
    ) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(env),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Env,
            env as *mut Obj,
        ))),
    }
}

pub unsafe fn free_env(env: *mut Env) -> Result<()> {
    free_handle(HandleType::Env, env as *mut Obj)
}

pub unsafe fn alloc_dbc(env: *mut Env) -> Result<*mut Dbc> {
    Ok(alloc_handle(HandleType::Dbc, env as *mut Obj)? as *mut Dbc)
}

pub unsafe fn free_dbc(dbc: *mut Dbc) -> Result<()> {
    free_handle(HandleType::Dbc, dbc as *mut Obj)
}

pub unsafe fn connect(dbc: *mut Dbc, dsn: &str, user: &str, pwd: &str) -> Result<()> {
    match SQLConnect(
        dbc,
        dsn.as_ptr(),
        dsn.len().try_into().unwrap(),
        user.as_ptr(),
        user.len().try_into().unwrap(),
        pwd.as_ptr(),
        pwd.len().try_into().unwrap(),
    ) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Dbc,
            dbc as *mut Obj,
        ))),
    }
}

pub unsafe fn disconnect(dbc: *mut Dbc) -> Result<()> {
    match SQLDisconnect(dbc) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Dbc,
            dbc as *mut Obj,
        ))),
    }
}

pub unsafe fn exec_direct(dbc: *mut Dbc, sql: &str) -> Result<()> {
    let stmt = alloc_stmt(dbc)?;

    match SQLExecDirect(stmt, sql.as_ptr(), sql.len().try_into().unwrap()) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Stmt,
            stmt as *mut Obj,
        ))),
    }?;

    free_stmt(stmt)?;

    Ok(())
}

pub unsafe fn alloc_stmt(dbc: *mut Dbc) -> Result<*mut Stmt> {
    Ok(alloc_handle(HandleType::Stmt, dbc as *mut Obj)? as *mut Stmt)
}

pub unsafe fn free_stmt(stmt: *mut Stmt) -> Result<()> {
    free_handle(HandleType::Stmt, stmt as *mut Obj)
}

pub unsafe fn prepare(stmt: *mut Stmt, sql: &str) -> Result<()> {
    match SQLPrepare(stmt, sql.as_ptr(), sql.len().try_into().unwrap()) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Stmt,
            stmt as *mut Obj,
        ))),
    }
}

pub unsafe fn bind_parameter<T>(stmt: *mut Stmt, parameter_number: u16, value: &mut T) -> Result<()>
where
    T: Parameter,
{
    match SQLBindParameter(
        stmt,
        parameter_number,
        ParamType::Input,
        value.value_type(),
        value.parameter_type(),
        value.column_size(),
        value.decimal_digits(),
        value.parameter_value_ptr(),
        value.buffer_length(),
        value.str_len_or_ind_ptr(),
    ) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Stmt,
            stmt as *mut Obj,
        ))),
    }
}

pub unsafe fn execute(stmt: *mut Stmt) -> Result<()> {
    match SQLExecute(stmt) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Stmt,
            stmt as *mut Obj,
        ))),
    }
}

pub unsafe fn fetch(stmt: *mut Stmt) -> Result<bool> {
    match SQLFetch(stmt) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(true),
        SqlReturn::NO_DATA => Ok(false),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Stmt,
            stmt as *mut Obj,
        ))),
    }
}

pub unsafe fn get_data<T>(stmt: *mut Stmt, col_or_param_num: u16, target: &mut T) -> Result<()>
where
    T: Parameter + ?Sized,
{
    match SQLGetData(
        stmt,
        col_or_param_num,
        target.value_type(),
        target.parameter_value_ptr(),
        target.buffer_length(),
        target.str_len_or_ind_ptr(),
    ) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Stmt,
            stmt as *mut Obj,
        ))),
    }
}

pub unsafe fn reset_stmt(stmt: *mut Stmt) -> Result<()> {
    match SQLFreeStmt(stmt, FreeStmtOption::Close) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
        _ => Err(Error::Diagnostics(get_diag_rec(
            HandleType::Stmt,
            stmt as *mut Obj,
        ))),
    }
}

pub trait Parameter {
    fn value_type(&self) -> CDataType;
    fn parameter_type(&self) -> SqlDataType;
    fn column_size(&self) -> usize;
    fn decimal_digits(&self) -> i16;
    fn parameter_value_ptr(&mut self) -> *mut c_void;
    fn buffer_length(&self) -> isize;
    fn str_len_or_ind_ptr(&mut self) -> *mut isize;
}

impl Parameter for u8 {
    fn value_type(&self) -> CDataType {
        CDataType::UTinyInt
    }

    fn parameter_type(&self) -> SqlDataType {
        SqlDataType::EXT_TINY_INT
    }

    fn column_size(&self) -> usize {
        0
    }

    fn decimal_digits(&self) -> i16 {
        0
    }

    fn parameter_value_ptr(&mut self) -> *mut c_void {
        self as *mut u8 as *mut c_void
    }

    fn buffer_length(&self) -> isize {
        1
    }

    fn str_len_or_ind_ptr(&mut self) -> *mut isize {
        ptr::null_mut()
    }
}

impl Parameter for u32 {
    fn value_type(&self) -> CDataType {
        CDataType::ULong
    }

    fn parameter_type(&self) -> SqlDataType {
        SqlDataType::INTEGER
    }

    fn column_size(&self) -> usize {
        10
    }

    fn decimal_digits(&self) -> i16 {
        0
    }

    fn parameter_value_ptr(&mut self) -> *mut c_void {
        self as *mut u32 as *mut c_void
    }

    fn buffer_length(&self) -> isize {
        4
    }

    fn str_len_or_ind_ptr(&mut self) -> *mut isize {
        ptr::null_mut()
    }
}

pub struct Char<'a> {
    bytes: &'a mut [u8],
    len: isize,
}

impl<'a> Char<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Char<'a> {
        let len = bytes.len() as isize - 1;
        Char { bytes, len }
    }
}

impl Parameter for Char<'_> {
    fn value_type(&self) -> CDataType {
        CDataType::Char
    }

    fn parameter_type(&self) -> SqlDataType {
        SqlDataType::VARCHAR
    }

    fn column_size(&self) -> usize {
        self.len as usize + 1
    }

    fn decimal_digits(&self) -> i16 {
        0
    }

    fn parameter_value_ptr(&mut self) -> *mut c_void {
        self.bytes.as_mut_ptr() as *mut c_void
    }

    fn buffer_length(&self) -> isize {
        self.len + 1
    }

    fn str_len_or_ind_ptr(&mut self) -> *mut isize {
        &mut self.len
    }
}
