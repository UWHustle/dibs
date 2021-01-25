use odbc_sys::{
    AttrOdbcVersion, CDataType, Dbc, Env, EnvironmentAttribute, FreeStmtOption, HandleType, Obj,
    ParamType, SQLAllocHandle, SQLBindParameter, SQLConnect, SQLDisconnect, SQLExecDirect,
    SQLExecute, SQLFetch, SQLFreeHandle, SQLFreeStmt, SQLGetData, SQLPrepare, SQLSetEnvAttr,
    SqlDataType, SqlReturn, Stmt,
};
use std::convert::TryInto;
use std::os::raw::c_void;
use std::{ptr, thread};

fn call_sys<F>(description: &str, mut f: F)
where
    F: FnMut() -> SqlReturn,
{
    match f() {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => (),
        SqlReturn(code) => {
            if !thread::panicking() {
                panic!("{} returned error code ({})", description, code)
            }
        }
    }
}

unsafe fn alloc_handle(handle_type: HandleType, input_handle: *mut Obj) -> *mut Obj {
    let mut handle = ptr::null_mut();

    call_sys("SQLAllocHandle", || {
        SQLAllocHandle(handle_type, input_handle, &mut handle)
    });

    handle
}

unsafe fn free_handle(handle_type: HandleType, handle: *mut Obj) {
    call_sys("SQLFreeHandle", || SQLFreeHandle(handle_type, handle));
}

pub unsafe fn alloc_env() -> *mut Env {
    let env = alloc_handle(HandleType::Env, ptr::null_mut()) as *mut Env;

    call_sys("SQLSetEnvAttr", || {
        SQLSetEnvAttr(
            env,
            EnvironmentAttribute::OdbcVersion,
            AttrOdbcVersion::Odbc3.into(),
            0,
        )
    });

    env
}

pub unsafe fn free_env(env: *mut Env) {
    free_handle(HandleType::Env, env as *mut Obj);
}

pub unsafe fn alloc_dbc(env: *mut Env) -> *mut Dbc {
    alloc_handle(HandleType::Dbc, env as *mut Obj) as *mut Dbc
}

pub unsafe fn free_dbc(dbc: *mut Dbc) {
    free_handle(HandleType::Dbc, dbc as *mut Obj)
}

pub unsafe fn connect(dbc: *mut Dbc, dsn: &str, user: &str, pwd: &str) {
    call_sys("SQLConnect", || {
        SQLConnect(
            dbc,
            dsn.as_ptr(),
            dsn.len().try_into().unwrap(),
            user.as_ptr(),
            user.len().try_into().unwrap(),
            pwd.as_ptr(),
            pwd.len().try_into().unwrap(),
        )
    });
}

pub unsafe fn disconnect(dbc: *mut Dbc) {
    call_sys("SQLDisconnect", || SQLDisconnect(dbc));
}

pub unsafe fn exec_direct(dbc: *mut Dbc, sql: &str) {
    let stmt = alloc_stmt(dbc);

    call_sys("SQLExecDirect", || {
        SQLExecDirect(stmt, sql.as_ptr(), sql.len().try_into().unwrap())
    });

    free_stmt(stmt);
}

pub unsafe fn alloc_stmt(dbc: *mut Dbc) -> *mut Stmt {
    alloc_handle(HandleType::Stmt, dbc as *mut Obj) as *mut Stmt
}

pub unsafe fn free_stmt(stmt: *mut Stmt) {
    free_handle(HandleType::Stmt, stmt as *mut Obj);
}

pub unsafe fn prepare(stmt: *mut Stmt, sql: &str) {
    call_sys("SQLPrepare", || {
        SQLPrepare(stmt, sql.as_ptr(), sql.len().try_into().unwrap())
    });
}

pub unsafe fn bind_parameter<T>(stmt: *mut Stmt, parameter_number: u16, value: &mut T)
where
    T: Parameter,
{
    call_sys("SQLBindParameter", || {
        SQLBindParameter(
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
        )
    });
}

pub unsafe fn execute(stmt: *mut Stmt) {
    call_sys("SQLExecute", || SQLExecute(stmt));
}

pub unsafe fn fetch(stmt: *mut Stmt) -> bool {
    match SQLFetch(stmt) {
        SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => true,
        SqlReturn::NO_DATA => false,
        SqlReturn(code) => panic!("SQLFetch returned error code ({})", code),
    }
}

pub unsafe fn get_data<T>(stmt: *mut Stmt, col_or_param_num: u16, target: &mut T)
where
    T: Parameter + ?Sized,
{
    call_sys("SQLGetData", || {
        SQLGetData(
            stmt,
            col_or_param_num,
            target.value_type(),
            target.parameter_value_ptr(),
            target.buffer_length(),
            target.str_len_or_ind_ptr(),
        )
    });
}

pub unsafe fn reset_stmt(stmt: *mut Stmt) {
    call_sys("SQLFreeStmt", || SQLFreeStmt(stmt, FreeStmtOption::Close));
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
