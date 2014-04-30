local odbx = require "opendbxuv"
local bit = require "bit"

p(odbx)

do
    local y = odbx.createHandle()
    getmetatable(y).__gc(y)
end

do
    local h = odbx.createHandle()
    odbx.setHandler(h, "error", function(...)
        p("error", ...)
        odbx.close(h)
    end)
    odbx.setHandler(h, "connect", function(...)
        p("connected", ...)
        local q = odbx.query(h, "SHOW TABLES", bit.bnot(0))
        p("Query start:", q)

        odbx.setHandler(q, "query", function(...)
            p("query", ...)
            odbx.fetch(q)
        end)

        odbx.setHandler(q, "row", function(t, ...)
            p("row", t, ...)
--             local q2 = odbx.query(h, ("SELECT * FROM %s"):format(t), bit.bnot(0))
--             odbx.setHandler(q2, "query", function()end)
--             odbx.setHandler(q2, "fetched", function()end)
--             odbx.setHandler(q2, "row", function(...)
--                 p("Row", t, ...)
--             end)
        end)

        odbx.setHandler(q, "fetched", function(...)
            p("fetched", ...)
            odbx.close(q)
            odbx.disconnect(h)
        end)


        p(odbx.getEnv(q))
    end)
    odbx.setHandler(h, "disconnect", function(...)
        p("Disconnected handle", ...) 
        odbx.close(h)
    end)

    odbx.connect(h, "mysql", "localhost", nil, "test", "test", "test")
end
