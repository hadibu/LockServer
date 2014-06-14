-- Copyright (c) hadibu

-- Created by IntelliJ IDEA.
-- User: chuanxin@163.com
-- Date: 14-6-6
-- Time: 上午9:50

--[[
-- 提供全局锁服务，每种锁都配置占有者上限，默认上限为1
-- 超出占有者上限的话lock阻塞住等待LockServer同意lock请求
 ]]

local LockUtil = {}
local socket = require("socket")

LockUtil.host = "127.0.0.1" -- 默认地址
LockUtil.port = 8888 -- 默认端口
LockUtil.timeout = 5000 -- 默认等待超时，毫秒


local PACKET_TYPE_LOCK = 1

local PACKET_HEAD_SIZE = 6

local function GenPacket(type, data)
    local packet = {}
    packet.Type = type

    if not data then
        data = 'nil'
    end

    packet.Data = data
    return packet
end

-- 申请锁，阻塞操作
-- @param lockName 锁的名字
-- @return lock成功返回socket对象供unlock使用，否则返回nil和错误信息
function LockUtil:lock(lockName)

    if lockName == nil then
        return nil, "lockName is nil"
    end

    local sock, err = socket.tcp()
    if not sock then
        return nil, err
    end
    local status, err = sock:connect(self.host, self.port)
    if not status then
        return nil, err
    end

    local json = require"json"
    local packet = GenPacket(PACKET_TYPE_LOCK, lockName)
    packet = json.encode(packet)

    packet = string.format("%0" .. PACKET_HEAD_SIZE .. "d", string.len(packet)) .. packet
    local sendlen, err = sock:send(packet)
    if sendlen == 0 or err then
        sock:close()
        return nil, err
    end

    -- 先读取包长
    local packetSize, err = sock:receive(PACKET_HEAD_SIZE)
    if err then
        sock:close()
        return nil, err
    end

    -- 再读取具体包数据
    local dataChunk, err = sock:receive(packetSize)
    if err then
        sock:close()
        return nil, err
    end
    --dataChunk = json.decode(dataChunk)
    --print("dataChunk = "..dataChunk)
    return sock
end

-- 释放锁，针对resty的话可以等待http会话结束自动解锁而不需手动调用unlock
-- @param lockName
-- @param sock 待解锁的socket，不可并发共享
-- @return It returns the 1 in case of success and returns nil with a string describing the error otherwise
function LockUtil:unlock(sock)

    if not sock then
        return nil, 'sock is nil'
    end

    return sock:close()
end

return LockUtil



