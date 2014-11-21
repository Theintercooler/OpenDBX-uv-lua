
local native = require "opendbxuv"
local ffi = require "ffi"

local Emitter
pcall(function() Emitter = require "luvit.core".Emitter end)
-- TODO: use own class implementation if used without luvit
assert(Emitter, "odbxuv's programmer is being lazy, he did not implement a custom Emitter implementation yet and therefore you are required to have luvit installed")

local NULL = {}

local Handle = Emitter:extend()
Handle.NULL = NULL

function Handle:initialize()
    self.handle = self.handle or native.createHandle()
end

function Handle:isNativeHandlerType(name)
    return false
end

function Handle:addHandlerType(name)
    if self:isNativeHandlerType(name) then
        native.setHandler(self.handle, name, function(...)
            self:emit(name, ...)
        end)
    end
end

function Handle:close(cb)
    if cb then
        self:once("close", cb)
    end

    if self.handle then
        native.close(self.handle)
        self.handle = nil
    end
end

local Connection = Handle:extend()

function Connection:isNativeHandlerType(type)
    return type == "connect" or type == "disconnect" or type == "error" or type == "close"
end

function Connection:connect(credentials, callback)
    self.type = credentials.type

    native.connect(
        self.handle,
        credentials.type,
        credentials.host or "localhost",
        credentials.port and credentials.port > 0 and credentials.port or nil,
        credentials.database,
        credentials.username,
        credentials.password)

    if callback then
        self:once("connect", function(...)
            callback(nil, self, ...)
        end)
        self:on("error", function(err)
            callback(err, self)
        end)
    end
end

function Connection:disconnect(callback)
    native.disconnect(self.handle)

    if callback then
        self:once("disconnect", callback)
    end
end

function Connection:escape(value, callback)
    local escapeHandle = native.escape(self.handle, value)

    native.setHandler(escapeHandle, "escape", function(...)
        local suc, err = pcall(callback, nil, ...)
        if not suc then
            if not pcall(callback, err) then
                self:emit("error", err)
            end
        end
        if escapeHandle then
            native.close(escapeHandle)
            escapeHandle = nil
        end
    end)

    native.setHandler(escapeHandle, "error", function(...)
        if escapeHandle then
            native.close(escapeHandle)
            escapeHandle = nil
        end
        callback(...)
    end)

    native.setHandler(escapeHandle, "close", function() end)
end

local Query = Handle:extend()

function Query:isNativeHandlerType(type)
    return type == "query" or type == "row" or type == "fetched" or type == "error" or type == "close" or type == "fetch"
end

function Connection:query(query, flags, callback)
    if type(flags) == "function" then
        callback = flags
        flags = nil
    end

    local q = native.query(self.handle, query, flags or 255)

    local wrappedQuery = Query:new(q)

    if callback then
        wrappedQuery:on("error", function(err)
            if type(err) == "table" then err.query = query end
            callback(err)
        end)
        wrappedQuery:once("query", function(...)
            callback(nil, wrappedQuery, ...)
        end)
    end

    return wrappedQuery
end

--Note: does not run parent constructor
function Query:initialize(q)
    self.handle = q
    Handle.initialize(self)
end

function Query:fetch(callback)
    native.fetch(self.handle)

    if callback then
        self:once("fetch", callback)
    end
end

function Query:getColumnCount()
    return native.queryColumnCount(self.handle)
end

function Query:getAffectedCount()
    return native.queryAffectedCount(self.handle)
end

function Query:getColumnInfo(i)
    return native.queryColumnInfo(self.handle, i)
end

function Query:getAllColumnInfo()
    local r = {}
    for i = 1, self:getColumnCount() do
        r[i] = self:getColumnInfo(i)
    end
    return r
end


local function createConnection(credentials, callback)
    local connection = Connection:new()
    connection:connect(credentials, callback)
    return connection
end


return {
    Emitter = Emitter,
    Connection = Connection,
    Query = Query,
    createConnection = createConnection,
    check = native.check
}