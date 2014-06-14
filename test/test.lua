-- Copyright (c) hadibu
-- Created by IntelliJ IDEA.
-- User: chuanxinfu@163.com
-- Date: 14-6-14
-- Time: 下午3:36

local locker = require("lock")

local sock, err = locker:lock("sql")
if err then
    print("lock err = " .. tostring(err))
end

local _, err = locker:unlock(sock)
if err then
    print("unlock err = " .. tostring(err))
end
