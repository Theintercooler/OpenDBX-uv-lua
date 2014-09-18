
local native = require "opendbxuv"
local ffi = require "ffi"

local Emitter
pcall(function() Emitter = require "luvit.core".Emitter end)
-- TODO: use own class implementation if used without luvit
assert(Emitter, "odbxuv's programmer is being lazy, he did not implement a custom Emitter implementation yet and therefore you are required to have luvit installed")

local Handle = Emitter:extend()

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

function Handle:close()
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
    native.connect(
        self.handle,
        credentials.type,
        credentials.host or "localhost",
        credentials.port and credentials.port > 0 and credentials.port or nil,
        credentials.database,
        credentials.username,
        credentials.password)

    if callback then
        self:once("connect", function(...) callback(self, ...) end)
    end
end

function Connection:disconnect(callback)
    native.disconnect(self.handle)

    if callback then
        self:once("disconnect", callback)
    end
end

local Query = Handle:extend()

function Query:isNativeHandlerType(type)
    return type == "query" or type == "row" or type == "fetched" or type == "error" or type == "close"
end

function Connection:query(query, flags, callback)
    if type(flags) == "function" then
        callback = flags
        flags = nil
    end

    local q = native.query(self.handle, query, flags or 255)

    local query = Query:new(q)

    if callback then
        query:once("query", function(...)
            callback(query, ...)
        end)
    end

    return query
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


local function createConnection(credentials, callback)
    local connection = Connection:new()
    connection:connect(credentials, callback)
    return connection
end


return {
    Connection = Connection,
    Query = Query,
    createConnection = createConnection,
    check = native.check
}