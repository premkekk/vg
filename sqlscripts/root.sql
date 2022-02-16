update mysql.user
set authentication_string=password('root'), password_expired='N'
where USER='root' and host = 'localhost'
;

flush privileges;
