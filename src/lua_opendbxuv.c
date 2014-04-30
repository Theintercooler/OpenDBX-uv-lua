#include <odbxuv/db.h>
#include <lua.h>
#include <lauxlib.h>
#include <malloc.h>
#include <assert.h>

#define META_TABLE "opendbxuv_handle"
#if 0
#define HANDLE_REF(L, handle, index)    do { printf("Handle ref: %p %i %s\n", handle, index, __PRETTY_FUNCTION__); _handle_ref(L, handle, index); } while(0);
#define HANDLE_UNREF(L, handle)         do { printf("Handle unref: %p %s\n", handle, __PRETTY_FUNCTION__); _handle_unref(L, handle); } while(0);
#else
#define HANDLE_REF(L, handle, index)    do { _handle_ref(L, handle, index); } while(0);
#define HANDLE_UNREF(L, handle)         do { _handle_unref(L, handle); } while(0);
#endif

void stackDump(lua_State *L)
{
    int i;
    for(i = -50; i <= 50; i++)
    {
        if(!lua_isnil(L, i))
        {
            printf("%i: %s (%s)\n", i, lua_tostring(L, i), lua_typename(L, lua_type(L, i)));
        }
    }
}

typedef struct {
    odbxuv_handle_t* handle; /* The actual opendbxuv handle. memory managed by us */
    int refCount;        /* a count of all pending request to know strength */
    lua_State* L;        /* L and ref together form a reference to the userdata */
    int threadref;       /* if handle is created in a coroutine(not main thread), threadref is
                            the reference to the coroutine in the Lua registery.
                            we release the reference when handle closed.
                            if handle is created in the main thread, threadref is LUA_NOREF.
                            we must hold the coroutine, because in some cases(see issue #319) that the coroutine
                            referenced by nothing and would collected by gc, then uv's callback touch an
                            invalid pointer. */
    int ref;             /* ref is null when refCount is 0 meaning we're weak */
    const char* type;
} lua_odbxuv_handle_t;

static lua_State* _get_main_thread(lua_State *L)
{
    lua_State *main_thread;

    lua_getfield(L, LUA_REGISTRYINDEX, "main_thread");
        main_thread = lua_tothread(L, -1);
    lua_pop(L, 1);

    return main_thread;
}

static uv_loop_t *_get_loop(lua_State *L)
{
    uv_loop_t *loop;

    lua_getfield(L, LUA_REGISTRYINDEX, "loop");
        loop = lua_touserdata(L, -1);
    lua_pop(L, 1);

    return loop;
}

static odbxuv_handle_t* _check_userdata(lua_State* L, int index, const char* type)
{
    /* Check for table wrappers as well and replace it with the userdata it points to */
    if (lua_istable (L, index)) {
        lua_getfield(L, index, "userdata");
        lua_replace(L, index);
    }

    luaL_checktype(L, index, LUA_TUSERDATA);

    return ((lua_odbxuv_handle_t *)lua_touserdata(L, index))->handle;
}

/* Initialize a new lhandle and push the new userdata on the stack. */
static lua_odbxuv_handle_t *_handle_create(lua_State* L, size_t size, const char* type)
{
    lua_State* mainthread;
    /* Create the userdata and set it's metatable */
    lua_odbxuv_handle_t* lhandle = (lua_odbxuv_handle_t*)lua_newuserdata(L, sizeof(lua_odbxuv_handle_t));

    /* Set metatable for type */
    luaL_getmetatable(L, META_TABLE);
    lua_setmetatable(L, -2);

    /* Create a local environment for storing stuff */
    lua_newtable(L);
    lua_setfenv (L, -2);

    /* Initialize and return the lhandle */
    lhandle->handle = (odbxuv_handle_t*)malloc(size);
    lhandle->handle->data = lhandle; /* Point back to lhandle from handle */
    lhandle->refCount = 0;
    lhandle->L = L;
    
    /* if handle create in a coroutine, we need hold the coroutine */
    mainthread = _get_main_thread(L);
    if (L != mainthread) {
        lua_pushthread(L);
        lhandle->threadref = luaL_ref(L, LUA_REGISTRYINDEX);
    } else {
        lhandle->threadref = LUA_NOREF;
    }
    lhandle->ref = LUA_NOREF;
    lhandle->type = type;
    return lhandle;
}

static void _handle_ref(lua_State* L, lua_odbxuv_handle_t* lhandle, int index)
{
    if (!lhandle->refCount)
    {
        lua_pushvalue(L, index);
        lhandle->ref = luaL_ref(L, LUA_REGISTRYINDEX);
    }
    lhandle->refCount++;
}

static void _handle_unref(lua_State* L, lua_odbxuv_handle_t* lhandle)
{
    lhandle->refCount--;
    assert(lhandle->refCount >= 0);

    if (!lhandle->refCount)
    {
        luaL_unref(L, LUA_REGISTRYINDEX, lhandle->ref);
        if (lhandle->threadref != LUA_NOREF)
        {
            luaL_unref(L, LUA_REGISTRYINDEX, lhandle->threadref);
            lhandle->threadref = LUA_NOREF;
        }
        lhandle->ref = LUA_NOREF;
    }
    assert(lhandle->refCount < 10);
}

#define _KILL_REFS(obj)                 \
if (obj->ref != LUA_NOREF)              \
{                                       \
    assert(obj->refCount);              \
    obj->refCount = 1;                  \
    HANDLE_UNREF(L, obj);     \
}                                       \
assert(obj->ref == LUA_NOREF);          \

static lua_State* _op_get_lua(lua_odbxuv_handle_t* lhandle)
{
    assert(lhandle->refCount);
    assert(lhandle->ref != LUA_NOREF);
    lua_rawgeti(lhandle->L, LUA_REGISTRYINDEX, lhandle->ref);
    return lhandle->L;
}

/* Meant as a lua_call replace for use in async callbacks
 * Uses the main loop and event source
 */
static void _acall(lua_State *C, int nargs, int nresults, const char* source) {
    lua_State* L;
    
    /* Get the main thread without cheating */
    lua_getfield(C, LUA_REGISTRYINDEX, "main_thread");
    L = lua_tothread(C, -1);
    lua_pop(C, 1);
    
    /* If C is not main then move to main */
    if (C != L) {
        lua_getglobal(L, "eventSource");
        lua_pushstring(L, source);
        lua_xmove (C, L, nargs + 1);
        lua_call(L, nargs + 2, nresults);
    } else {
        
        /* Wrap the call with the eventSource function */
        int offset = nargs + 2;
        lua_getglobal(L, "eventSource");
        lua_insert(L, -offset);
        lua_pushstring(L, source);
        lua_insert(L, -offset);
        lua_call(L, nargs + 2, nresults);
    }
}

static void _emit_event(lua_State* L, const char* name, int nargs)
{
    /* Load the connection callback */
    lua_getfenv(L, -nargs - 1);
    lua_getfield(L, -1, name);
    /* remove the userdata environment */
    lua_remove(L, -2);
    /* Remove the userdata */
    lua_remove(L, -nargs - 2);

    if (lua_isfunction (L, -1) == 0) {
        lua_pop(L, 1 + nargs);
        fprintf(stderr, "odbxuv-lua Warning: No callback function named: %s\n", name);
        return;
    }

    /* move the function below the args */
    lua_insert(L, -nargs - 1);
    _acall(L, nargs, 0, name);
}

/* Registers a callback, callback_index can't be negative */
static void _register_event(lua_State* L, int userdata_index, const char* name, int callback_index)
{
    lua_getfenv(L, userdata_index);
    lua_pushvalue(L, callback_index);
    lua_setfield(L, -2, name);
    lua_pop(L, 1);
}



/* Pushes an error object onto the stack */
static void _push_async_error_raw(lua_State* L, int code, int type, const char *msg, const char* source, const char* path)
{
    lua_newtable(L);
    lua_getglobal(L, "errorMeta");
    lua_setmetatable(L, -2);
    
    if (path) {
        lua_pushstring(L, path);
        lua_setfield(L, -2, "path");
        lua_pushfstring(L, "%d, %s '%s'", code, msg, path);
    } else {
        lua_pushfstring(L, "%d, %s", code, msg);
    }
    lua_setfield(L, -2, "message");

    const char *v = NULL;
    switch(-code)
    {
        case ODBX_ERR_SUCCESS:
            v = v ? v : "SUCCESS";
        case ODBX_ERR_BACKEND:
            v = v ? v : "BACKEND";
        case ODBX_ERR_NOCAP:
            v = v ? v : "NOCAP";
        case ODBX_ERR_PARAM:
            v = v ? v : "PARAM";
        case ODBX_ERR_NOMEM:
            v = v ? v : "NOMEM";
        case ODBX_ERR_SIZE:
            v = v ? v : "SIZE";
        case ODBX_ERR_NOTEXIST:
            v = v ? v : "NOTEXISTS";
        case ODBX_ERR_NOOP:
            v = v ? v : "NOOP";
        case ODBX_ERR_OPTION:
            v = v ? v : "OPTION";
        case ODBX_ERR_OPTRO:
            v = v ? v : "OPTRO";
        case ODBX_ERR_OPTWR:
            v = v ? v : "OPTWR";
        case ODBX_ERR_RESULT:
            v = v ? v : "RESULT";
        case ODBX_ERR_NOTSUP:
            v = v ? v : "NOTSUP";
        case ODBX_ERR_HANDLE:
            v = v ? v : "HANDLE";
            lua_pushstring(L, v);
        break;

        default:
        lua_pushnumber(L, code);
        break;
    }
    lua_setfield(L, -2, "code");

    lua_pushnumber(L, type);
    lua_setfield(L, -2, "type");
    
    lua_pushstring(L, source);
    lua_setfield(L, -2, "source");
}

static void _push_async_error(lua_State* L, odbxuv_handle_t *op, const char* source, const char* path)
{
    _push_async_error_raw(L, op->error->error, op->error->errorType, op->error->errorString, source, path);
}

static void _handle_close(odbxuv_handle_t* handle)
{
    odbxuv_free_handle(handle);
    free(handle);
}

static int _close_handle(odbxuv_handle_t *handle)
{
    odbxuv_close(handle, _handle_close);
    return 0;
}

static int _handle_gc(lua_State* L)
{
    lua_odbxuv_handle_t* lhandle = (lua_odbxuv_handle_t*)lua_touserdata(L, 1);

    if (lhandle->handle)
    {
        fprintf(stderr, "WARNING: forgot to close %s lhandle=%p handle=%p\n", lhandle->type, lhandle, lhandle->handle);
        _close_handle(lhandle->handle);

        _KILL_REFS(lhandle)
        lhandle->handle->data = NULL;
        lhandle->handle = NULL;
    }
    return 0;
}

void odbxuv_lua_error(lua_State *L, int error, const char *message)
{
    if (!message)
    {
        message = odbx_error(NULL, error);
    }

    luaL_error(L, message);
}

odbxuv_connection_t* _create_connection(lua_State* L) {
    return (odbxuv_connection_t*)_handle_create(L, sizeof(odbxuv_connection_t), "odbxuv_connection_t")->handle;
}

odbxuv_op_query_t* _create_query(lua_State* L) {
    return (odbxuv_op_query_t*)_handle_create(L, sizeof(odbxuv_op_query_t), "odbxuv_op_query_t")->handle;
}

int odbxuv_lua_create_handle(lua_State *L)
{
    odbxuv_connection_t *connection = _create_connection(L); 
    odbxuv_init_connection(connection, _get_loop(L));
    return 1;
}

static int odbxuv_lua_set_handler(lua_State* L)
{
    const char* name;
    _check_userdata(L, 1, "handle");
    name = luaL_checkstring(L, 2);
    luaL_checktype(L, 3, LUA_TFUNCTION);

    _register_event(L, 1, name, 3);

    return 0;
}

static void _lua_after_connect(odbxuv_op_connect_t *op, int status)
{
    /* load the lua state and the userdata */
    lua_State* L = _op_get_lua(op->connection->data);

    if (status < ODBX_ERR_SUCCESS)
    {
        _push_async_error(L, (odbxuv_handle_t *)op, "after_connect", NULL);
        _emit_event(L, "error", 1);
    }
    else
    {
        _emit_event(L, "connect", 0);
    }
    HANDLE_UNREF(L, op->connection->data);
    odbxuv_free_error((odbxuv_handle_t *)op);
    free(op);
}

int odbxuv_lua_connect(lua_State* L)
{
    odbxuv_connection_t *handle = (odbxuv_connection_t *)_check_userdata(L, 1, "odbxuv_connection_t");

    const char *backend = luaL_checkstring(L, 2);
    const char *host    = luaL_checkstring(L, 3);
    const char *port    = lua_tostring(L, 4);

    const char *database= luaL_checkstring(L, 5);
    const char *user    = luaL_checkstring(L, 6);
    const char *password= luaL_checkstring(L, 7);

    int method          = lua_tonumber(L, 8);

    odbxuv_op_connect_t *op = malloc(sizeof(odbxuv_op_connect_t));

    op->backend = backend;
    op->host = host;
    op->port = port;
    op->database = database;
    op->password = password;
    op->user = user;
    op->method = method;

    int err = odbxuv_connect(handle, op, _lua_after_connect);

    if (err < ODBX_ERR_SUCCESS)
    {
        free(op);
        return luaL_error(L, "odbxuv_connect: %i", err);
    }

    HANDLE_REF(L, handle->data, 1);

    return 0;
}

static void _lua_after_query(odbxuv_op_query_t *op, int status)
{
    lua_State* L = _op_get_lua(op->data);

    if (status < ODBX_ERR_SUCCESS)
    {
        _push_async_error(L, (odbxuv_handle_t *)op, "after_query", NULL);
        _emit_event(L, "error", 1);
    }
    else
    {
        _emit_event(L, "query", 0);

    }

    HANDLE_UNREF(L, op->data);

    odbxuv_free_error((odbxuv_handle_t *)op);
}

int odbxuv_lua_query(lua_State *L)
{
    odbxuv_connection_t *handle = (odbxuv_connection_t *)_check_userdata(L, 1, "odbxuv_connection_t");

    const char *queryString = luaL_checkstring(L, 2);
    int flags = lua_tonumber(L, 3);

    if(handle->status != ODBXUV_CON_STATUS_CONNECTED)
    {
        luaL_error(L, "Handle is not connected!\n");
    }

    odbxuv_op_query_t *query = _create_query(L);

    int err = odbxuv_query(handle, query, queryString, flags, _lua_after_query);

    if (err < ODBX_ERR_SUCCESS)
    {
        _handle_close((odbxuv_handle_t *)query);
        return luaL_error(L, "odbxuv_connect: %i", err);
    }

    HANDLE_REF(L, query->data, -1);

    return 1;
}

void _lua_after_fetch(odbxuv_op_query_t *result, odbxuv_row_t *row, int status)
{
    lua_State* L = _op_get_lua(result->data);

    //TODO: handle fetch errors

    if(row)
    {
        int i = 0;
        if(row->value)
        {
            for(i = 0; i < result->columnCount; i++)
            {
                lua_pushstring(L, row->value[i]);
            }
        }
        _emit_event(L, "row", i);
    }
    else
    {
        HANDLE_UNREF(L, result->data);
        _emit_event(L, "fetched", 0);
    }
}

int odbxuv_lua_fetch(lua_State *L)
{
    odbxuv_op_query_t *handle = (odbxuv_op_query_t *)_check_userdata(L, 1, "odbxuv_op_query_t");
    odbxuv_query_process(handle, _lua_after_fetch);

    HANDLE_REF(L, handle->data, 1);

    return 0;
}

static void _lua_after_disconnect(odbxuv_op_disconnect_t *op, int status)
{
    lua_odbxuv_handle_t *lhandle = (lua_odbxuv_handle_t *)op->connection->data;
    /* load the lua state and the userdata */
    lua_State* L = _op_get_lua(lhandle);

    HANDLE_UNREF(L, lhandle);

    if (status < ODBX_ERR_SUCCESS)
    {
        _push_async_error(L, (odbxuv_handle_t *)op, "after_disconnect", NULL);
        _emit_event(L, "error", 1);
        odbxuv_free_error((odbxuv_handle_t *)op);
    }
    else
    {
        _emit_event(L, "disconnect", 0);
    }
}

int odbxuv_lua_disconnect(lua_State *L)
{
    odbxuv_connection_t *handle = (odbxuv_connection_t *)_check_userdata(L, 1, "odbxuv_connection_t");

    if(handle->status != ODBXUV_CON_STATUS_CONNECTED)
    {
        luaL_error(L, "Handle is not connected!\n");
    }

    odbxuv_op_disconnect_t *op = (odbxuv_op_disconnect_t *)malloc(sizeof(odbxuv_op_disconnect_t));

    odbxuv_disconnect(handle, op, _lua_after_disconnect);

    HANDLE_REF(L, handle->data, 1);

    return 0;
}

int odbxuv_lua_close (lua_State *L)
{
    odbxuv_handle_t *handle = (odbxuv_handle_t *)_check_userdata(L, 1, "odbxuv_handle_t");
    lua_odbxuv_handle_t *lhandle = (lua_odbxuv_handle_t *)handle->data;

    if(lhandle == 0)
    {
        luaL_error(L, "Handle is already closed.");
    }

    _close_handle(handle);

    _KILL_REFS(lhandle)
    lhandle->handle = NULL;

    return 0;
}

int odbxuv_lua_get_env(lua_State *L)
{
    lua_getfenv(L, 1);
    return 1;
}

static const luaL_reg functions[] = {
    { "setHandler", odbxuv_lua_set_handler },
    { "createHandle",   odbxuv_lua_create_handle },
    { "connect", odbxuv_lua_connect },
    { "query", odbxuv_lua_query },
    { "fetch", odbxuv_lua_fetch},
    { "disconnect", odbxuv_lua_disconnect },
    { "close", odbxuv_lua_close },
    { "getEnv", odbxuv_lua_get_env },
    { NULL, NULL }
};

int luaopen_opendbxuv (lua_State* L)
{
    /* metatable for handle userdata types */
    /* It is it's own __index table to save space */
    luaL_newmetatable(L, META_TABLE);
    lua_pushcfunction(L, _handle_gc);
    lua_setfield(L, -2, "__gc");
    lua_pop(L, 1);

    lua_newtable (L);

    luaL_register(L, NULL, functions);

    lua_pushstring(L, "Unkown version"); //TODO: implement
    lua_setfield(L, -2, "VERSION");

    return 1;
}

#ifdef LIB_SHARED
LUALIB_API int luaopen_lua (lua_State *L)
{
    return luaopen_opendbxuv(L);
}
#endif