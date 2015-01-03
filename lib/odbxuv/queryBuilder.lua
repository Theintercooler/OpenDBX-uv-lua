
local table = require "table"
local ibmt = require "ibmt"

local odbxuv = require "odbxuv"

local QueryBuilder = odbxuv.Emitter:extend()

function QueryBuilder:initialize(connection)
    self.connection = connection
end

function QueryBuilder:select(...)
    self.what = {...}
    self.what = #self.what > 0 and self.what or {{"*"}}
    self.finalize = self.finalizeSelect
end

function QueryBuilder:insert(...)
    self.what = {...}
    self.finalize = self.finalizeInsert
end

function QueryBuilder:ignore()
    self.ignore = true
end

function QueryBuilder:from(...)
    self.from = {...}
end

function QueryBuilder:into(...)
    self:from(...)
end

function QueryBuilder:values(...)
    self.rows = self.rows or {}
    self.rows[#self.rows+1] = {...}
end

function QueryBuilder:where(tree)
    self.whereCondition = self.whereCondition or {}
    self.whereCondition.tree = tree
end

function QueryBuilder:bind(var, value)
    self.whereCondition = self.whereCondition or {}
    self.whereCondition.vars = self.whereCondition.vars or {}
    self.whereCondition.vars[var] = value
end

function QueryBuilder:limit(limit)
    self.limited = limit
end

function QueryBuilder:escapeFieldName(field)
    if type(field) == "table" then
        return field[1] .. "." .. self:escapeFieldName(field[2])
    else
        return "'"..field:gsub("\'", "\\'").."'"
    end
end

function QueryBuilder:createEscapedFieldList(fields)
    local out = {}
    
    for i, field in ipairs(fields) do
        if type(field) == "table" and field[1] == "*" then
            out[i] = "*"
        else
            out[i] = self:escapeFieldName(field)
        end
    end
    
    return table.concat(out, ", ")
end

function QueryBuilder:escapeTableName(table)
    return type(table) == "table" and table[1] .. "." .. table[2] or table
end

function QueryBuilder:createEscapedTableList(tables)
    local out = {}
    
    for i, table in ipairs(tables) do
        out[i] = self:escapeTableName(table)
    end
    
    return table.concat(out, ", ")
end

function QueryBuilder:escapeValue(value, callback)
    if type(value) == "number" then
        callback(nil, value)
    else
        self.connection:escape(value, callback)
    end
end

function QueryBuilder:quoteValue(value, callback)
    if value == self.connection.NULL then
        return callback(nil, "NULL")
    end
    self:escapeValue(value, function(err, data)
        callback(err, type(data) == "string" and "'"..data.."'" or data)
    end)
end

function QueryBuilder:createEscapedWhereTree(tree, cb)
    local condition = tree.tree
    local task = ibmt.create()

    task:push()

    for variable, value in pairs(tree.vars) do
        task:push()
        self:quoteValue(value, function(err, val)
            if err then
                return task:cancel(err)
            end

            condition = condition:gsub(":"..variable, val)
            task:pop()
        end)
    end

    task:on("finish", function() p "finish" cb(nil, condition) end)
    task:on("error", cb)

    task:pop()
end

function QueryBuilder:finalizeInsert(cb)
    local task = ibmt.create()
    local value = {}
    value[1] = "INSERT " .. (self.ignore == true and "IGNORE " or "") .. "INTO"
    value[2] = self:escapeTableName(self.from[1])
    value[3] = "(".. self:createEscapedFieldList(self.what) .. ")"
    value[4] = "VALUES "
    
    task:push()
    local first = true
    for _, row in pairs(self.rows) do
        value[#value+1] = first and "(" or ",("

            local first2 = true
            for __, cell in pairs(row) do
                local dest = #value+1
                local wasFirst = first2
                value[dest] = "<ESCAPING>"
                task:push()
                self:quoteValue(cell, function(err, val)
                    if err then
                        return task:cancel(err)
                    end
                    value[dest] = wasFirst and "  "..val or ", "..val
                    task:pop()
                end)
                first2 = false
            end
        value[#value+1] = ")"
        first = false
    end
    value[#value+1] = ";"
    
    if cb then
        task:on("finish", function()
        cb(nil, table.concat(value, "\n"))
    end)
    task:on("error", function(...)
    cb(...)
end)
    end
    
    task:pop()
    
    return task
end

function QueryBuilder:finalizeSelect(cb)
    local task = ibmt.create()
    local value = {}
    value[1] = "SELECT ".. self:createEscapedFieldList(self.what)
    value[2] = "FROM "  .. self:createEscapedTableList(self.from)
    
    task:push()
    
    if self.whereCondition then
        task:push()
        self:createEscapedWhereTree(self.whereCondition, function(err, data)
            if err then
                task:error(err)
            end
            value[3] = "WHERE " .. data
            task:pop()
        end)
    else
        value[3] = ""
    end

    if self.order then
        value[4] = "ORDER BY "
        ..(self.order.raw and self.order.raw or self:createEscapedFieldList(self.order.fields))
        ..(self.order.direction and "\n" .. self:createOrderDirection(self.order.direction) or "")
    else
        value[4] = ""
    end

    if self.limited then
        value[5] = "LIMIT "..self.limited
    else
        value[5] = ""
    end

    if cb then
        task:on("finish", function()
            cb(nil, table.concat(value, "\n"))
        end)
        task:on("error", function(...)
            cb(...)
        end)
    end
    
    task:pop()
    
    return task
end

local MySQLQueryBuilder = QueryBuilder:extend()

function MySQLQueryBuilder:escapeFieldName(field)
    if type(field) ~= "string" then
        return QueryBuilder.escapeFieldName(self, field)
    else
        return '`'..field:gsub("`", "\\`")..'`'
    end
end

local function createQueryBuilder(connection, ...)
    if connection.type == "mysql" then
        return MySQLQueryBuilder:new(connection, ...)
    else
        error "Unsupported database type for query builder."
    end
end

return {
    QueryBuilder= QueryBuilder,
    createQueryBuilder = createQueryBuilder
}
