//
//  lua_unqlite.c
//
//  Created by pigsoldier on 15/2/9.
//
//

#ifdef __cplusplus
extern "C" {
#endif
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"

    
#include "unqlite.h"
    
#ifdef __cplusplus
}
#endif



#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#define SETLITERAL(n,v) (lua_pushliteral(L, n), lua_pushliteral(L, v), lua_settable(L, -3))
#define SETINT(n,v) (lua_pushliteral(L, n), lua_pushinteger(L, v), lua_settable(L, -3))

static void Fatal(lua_State* L, unqlite *pDb,const char *zMsg)
{
    if( pDb ){
        const char *zErr;
        int iLen = 0; /* Stupid cc warning */
        
        /* Extract the database error log */
        unqlite_config(pDb,UNQLITE_CONFIG_ERR_LOG,&zErr,&iLen);
        if( iLen > 0 ){
            luaL_error(L, zErr);
        }
    }else{
        if( zMsg ){
            luaL_error(L, zMsg);
        }
    }

}

static int _unqlite_open(lua_State* L) {
    const char* file_name = luaL_checkstring(L, 1);
    unsigned int i_mode = luaL_optint(L, 2, 0);
    
    if (i_mode == 0)
        i_mode = UNQLITE_OPEN_CREATE | UNQLITE_OPEN_READWRITE;
    
    unqlite *pDb;
    int rc = unqlite_open(&pDb, file_name, i_mode);
    
    if (rc != UNQLITE_OK) {
        Fatal(L, pDb, "[Unqlite_Lua] Out of memory");
        lua_pushboolean(L,0);
        return 0;
    } else {
        lua_pushlightuserdata(L, pDb);
        return 1;
        
    }
}

static int _unqlite_close(lua_State* L) {
    unqlite* pDb = (unqlite*)lua_touserdata(L, 1);
    if (NULL == pDb) {
        luaL_argerror(L, 1,  "[Unqlite_Lua] close get null pDb");
        lua_pushboolean(L,0);
        return 0;
    }
    
    int rs = unqlite_close(pDb);
    if (rs != UNQLITE_OK) {
        Fatal(L, pDb, "[Unqlite_Lua] Out of memory");
        lua_pushboolean(L,0);
        return 0;
    } else {
		lua_pushboolean(L,1);
        return 1;
    }

}

static int _unqlite_kv_store(lua_State* L) {
    unqlite* pDb = (unqlite*)lua_touserdata(L, 1);
    if (NULL == pDb) {
        luaL_argerror(L, 1,  "[Unqlite_Lua] close get null pDb");
        lua_pushboolean(L,0);
        return 0;
    }
    
    size_t keyLen;
    void* key = (void* )luaL_checklstring(L, 2, &keyLen);
    size_t contentLen;
    void* content = (void* )luaL_checklstring(L, 3, &contentLen);
    
    if (keyLen <= 0 || contentLen <= 0 || key == NULL || content == NULL) {
        luaL_argerror(L, 2, "[Unqlite_lua]");
        lua_pushboolean(L,0);
        return 0;
    }
    
    int rs;
    
    rs = unqlite_kv_store(pDb, key, keyLen, content, contentLen);
    
    if (rs != UNQLITE_OK) {
        Fatal(L, pDb, "[Unqlite_Lua] K-V store failed");
        lua_pushboolean(L,0);
        return 0;
    } else {
        lua_pushboolean(L,1);
        return 1;
    }
    
}

static int _unqlite_kv_append(lua_State* L) {
    unqlite* pDb = (unqlite*)lua_touserdata(L, 1);
    if (NULL == pDb) {
        luaL_argerror(L, 1,  "[Unqlite_Lua] close get null pDb");
        lua_pushboolean(L,0);
        return 0;
    }

    
    size_t keyLen;
    void* key = (void* )luaL_checklstring(L, 2, &keyLen);
    size_t contentLen;
    void* content = (void* )luaL_checklstring(L, 3, &contentLen);
    
    if (keyLen <= 0 || contentLen <= 0 || key == NULL || content == NULL) {
        luaL_argerror(L, 2, "[Unqlite_lua]");
        lua_pushboolean(L,0);
        return 0;
    }
    
    int rs;
    
    rs = unqlite_kv_append(pDb, key, keyLen, content, contentLen);
    
    if (rs != UNQLITE_OK) {
        Fatal(L, pDb, "[Unqlite_Lua] Out of memory");
        lua_pushboolean(L,0);
        return 0;
    } else {
        lua_pushboolean(L,1);
        return 1;
    }
}

static int _unqlite_kv_fetch(lua_State* L) {
    unqlite* pDb = (unqlite*)lua_touserdata(L, 1);
    if (NULL == pDb) {
        luaL_argerror(L, 1,  "[Unqlite_Lua] close get null pDb");
        lua_pushboolean(L,0);
        return 0;
    }
    
    size_t keyLen;
    void* key = (void* )luaL_checklstring(L, 2, &keyLen);
    
    if (keyLen <= 0 || key == NULL ) {
        luaL_argerror(L, 2, "[Unqlite_lua]");
        lua_pushboolean(L,0);
        return 0;
    }
    
    unqlite_int64 bufLen = 0;
    unqlite_kv_fetch(pDb, key, keyLen, NULL, &bufLen);
    
    char* pBuf = (char *)malloc(sizeof(char)*bufLen);
    unqlite_kv_fetch(pDb, key, keyLen, pBuf, &bufLen);
    
    if (pBuf == NULL) {
        printf("pBuf is nil");
        lua_pushnil(L);
    } else {
        lua_pushlstring(L, (const char *)pBuf, bufLen);
        free(pBuf);
    }
    
    return 1;
}

static int _unqlite_kv_delete(lua_State* L) {
    unqlite* pDb = (unqlite*)lua_touserdata(L, 1);
    if (NULL == pDb) {
        luaL_argerror(L, 1,  "[Unqlite_Lua] close get null pDb");
        lua_pushboolean(L,0);
        return 0;
    }
    
    size_t keyLen;
    void* key = (void* )luaL_checklstring(L, 2, &keyLen);
    
    if (keyLen <= 0 || key == NULL ) {
        luaL_argerror(L, 2, "[Unqlite_lua]");
        lua_pushboolean(L,0);
        return 0;
    }
    
    int rs = unqlite_kv_delete(pDb, key, keyLen);
    
    if (rs != UNQLITE_OK) {
        Fatal(L, pDb, "[Unqlite_Lua] Out of memory");
        lua_pushboolean(L,0);
        return 0;
    } else {
        lua_pushboolean(L,1);
        return 1;
    }
}

static int _unqlite_begin(lua_State* L) {
    unqlite* pDb = (unqlite*)lua_touserdata(L, 1);
    if (NULL == pDb) {
        luaL_argerror(L, 1,  "[Unqlite_Lua] close get null pDb");
        lua_pushboolean(L,0);
        return 0;
    }
    
    int rs = unqlite_begin(pDb);
    if (rs != UNQLITE_OK) {
        Fatal(L, pDb, "[Unqlite_Lua] Out of memory");
        lua_pushboolean(L,0);
        return 0;
    } else {
        lua_pushboolean(L,1);
        return 1;
    }
    

}

static int _unqlite_commit(lua_State* L) {
    unqlite* pDb = (unqlite*)lua_touserdata(L, 1);
    if (NULL == pDb) {
        luaL_argerror(L, 1,  "[Unqlite_Lua] close get null pDb");
        lua_pushboolean(L,0);
        return 0;
    }
    
    int rs = unqlite_commit(pDb);
    if (rs != UNQLITE_OK) {
        Fatal(L, pDb, "[Unqlite_Lua] Out of memory");
        lua_pushboolean(L,0);
        return 0;
    } else {
        lua_pushboolean(L,1);
        return 1;
    }
}

static int _unqlite_rollback(lua_State* L) {
    unqlite* pDb = (unqlite*)lua_touserdata(L, 1);
    if (NULL == pDb) {
        luaL_argerror(L, 1,  "[Unqlite_Lua] close get null pDb");
        lua_pushboolean(L,0);
        return 0;
    }
    
    int rs = unqlite_rollback(pDb);
    if (rs != UNQLITE_OK) {
        Fatal(L, pDb, "[Unqlite_Lua] Out of memory");
        lua_pushboolean(L,0);
        return 0;
    } else {
        lua_pushboolean(L,1);
        return 1;
    }
}

static const luaL_Reg unqlite_functions[] = {
    { "open", _unqlite_open },
    { "close", _unqlite_close },
    { "store", _unqlite_kv_store },
    { "fetch", _unqlite_kv_fetch },
    { "delete", _unqlite_kv_delete },
    { "delete", _unqlite_kv_delete },
    
    { NULL,      NULL           }
    
};


int lua_open_unqlite(lua_State* L) {
    luaL_register(L, "unqlite", unqlite_functions);

    SETINT("UNQLITE_OPEN_READONLY", UNQLITE_OPEN_READONLY);
    SETINT("UNQLITE_OPEN_READWRITE", UNQLITE_OPEN_READWRITE);
    SETINT("UNQLITE_OPEN_CREATE", UNQLITE_OPEN_CREATE);
    SETINT("UNQLITE_OPEN_EXCLUSIVE", UNQLITE_OPEN_EXCLUSIVE);
    SETINT("UNQLITE_OPEN_TEMP_DB", UNQLITE_OPEN_TEMP_DB);
    SETINT("UNQLITE_OPEN_NOMUTEX", UNQLITE_OPEN_NOMUTEX);
    SETINT("UNQLITE_OPEN_OMIT_JOURNALING", UNQLITE_OPEN_OMIT_JOURNALING);
    SETINT("UNQLITE_OPEN_IN_MEMORY", UNQLITE_OPEN_IN_MEMORY);
    SETINT("UNQLITE_OPEN_MMAP", UNQLITE_OPEN_MMAP);
    return 1;
}
