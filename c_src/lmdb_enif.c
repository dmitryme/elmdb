#include <erl_nif.h>
#include <stdint.h>
#include "lmdb.h"
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

ERL_NIF_TERM ok_atom;
ERL_NIF_TERM err_atom;
ERL_NIF_TERM not_found_atom;
ERL_NIF_TERM depth_atom;
ERL_NIF_TERM branch_pages_atom;
ERL_NIF_TERM leaf_pages_atom;
ERL_NIF_TERM overflow_pages_atom;
ERL_NIF_TERM entries_atom;
ErlNifResourceType* env_res_type;
ErlNifResourceType* txn_res_type;
ErlNifResourceType* cur_res_type;

typedef struct
{
   int32_t env_flags;
   int64_t map_size;
   int32_t max_readers;
   int32_t db_flags;
   char    path[256];
} mdb_env_options_t;

typedef struct
{
   MDB_env*          env;
   MDB_dbi           dbi;
   mdb_env_options_t options;
} mdb_env_res_t;

typedef struct
{
   mdb_env_res_t* env;
   MDB_txn*       txn;
} mdb_txn_res_t;

typedef struct
{
   MDB_cursor* cur;
} mdb_cur_res_t;

#define check(exp, txt, ...)                       \
   if (exp)                                        \
   {                                               \
      err = make_error(env, txt, ##__VA_ARGS__);   \
      goto error;                                  \
   }

#define check2(exp, err, txt, ...)                 \
   if (exp)                                        \
   {                                               \
      (err) = make_error(env, txt, ##__VA_ARGS__); \
      goto error;                                  \
   }

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM make_error(ErlNifEnv* env, char const* err_msg, ...)
{
   va_list ap;
   va_start(ap, err_msg);
   char text[1024];
   vsnprintf(text, sizeof(text), err_msg, ap);
   return enif_make_tuple2(env, err_atom, enif_make_string(env, text, ERL_NIF_LATIN1));
}

//--------------------------------------------------------------------------------------------------------------------//
static void env_destructor(ErlNifEnv* env, void* obj)
{
   mdb_env_res_t* env_res = (mdb_env_res_t*)obj;
   if (env_res->env)
   {
      mdb_env_close(env_res->env);
      env_res->env = NULL;
   }
}

//--------------------------------------------------------------------------------------------------------------------//
static void txn_destructor(ErlNifEnv* env, void* obj)
{
   mdb_txn_res_t* txn_res = (mdb_txn_res_t*)obj;
   if (txn_res->txn && txn_res->env->env)
   {
      mdb_txn_abort(txn_res->txn);
      txn_res->txn = NULL;
      enif_release_resource(txn_res->env);
   }
}

//--------------------------------------------------------------------------------------------------------------------//
static void cur_destructor(ErlNifEnv* env, void* obj)
{
   mdb_cur_res_t* cur_res = (mdb_cur_res_t*)obj;
   if (cur_res->cur)
   {
      mdb_cursor_close(cur_res->cur);
      cur_res->cur = NULL;
   }
}

//--------------------------------------------------------------------------------------------------------------------//
static int32_t on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
   ok_atom             = enif_make_atom(env, "ok");
   err_atom            = enif_make_atom(env, "error");
   not_found_atom      = enif_make_atom(env, "not_found");
   depth_atom          = enif_make_atom(env, "depth");
   branch_pages_atom   = enif_make_atom(env, "branch_pages");
   leaf_pages_atom     = enif_make_atom(env, "leaf_pages");
   overflow_pages_atom = enif_make_atom(env, "overflow_pages");
   entries_atom        = enif_make_atom(env, "entries");
   env_res_type        = enif_open_resource_type(env, NULL, "mdb_env", env_destructor, ERL_NIF_RT_CREATE, NULL);
   txn_res_type        = enif_open_resource_type(env, NULL, "mdb_txn", txn_destructor, ERL_NIF_RT_CREATE, NULL);
   cur_res_type        = enif_open_resource_type(env, NULL, "mdb_cur", cur_destructor, ERL_NIF_RT_CREATE, NULL);
   return 0;
}

//--------------------------------------------------------------------------------------------------------------------//
static int32_t read_env_flags(ErlNifEnv* env, ERL_NIF_TERM list)
{
   ERL_NIF_TERM head;
   ERL_NIF_TERM tail;
   int32_t res = enif_get_list_cell(env, list, &head, &tail);
   int32_t flags = 0;
   while(res)
   {
      char buf[64] = {};
      if (!enif_get_atom(env, head, buf, sizeof(buf), ERL_NIF_LATIN1))
      {
         return -1;
      }
      if (!strcmp(buf, "fixed_map"))       flags |= MDB_FIXEDMAP;
      else if (!strcmp(buf, "nosubdir"))   flags |= MDB_NOSUBDIR;
      else if (!strcmp(buf, "rdonly"))     flags |= MDB_RDONLY;
      else if (!strcmp(buf, "writemap"))   flags |= MDB_WRITEMAP;
      else if (!strcmp(buf, "nometasync")) flags |= MDB_NOMETASYNC;
      else if (!strcmp(buf, "nosync"))     flags |= MDB_NOSYNC;
      else if (!strcmp(buf, "mapasync"))   flags |= MDB_MAPASYNC;
      else if (!strcmp(buf, "notls"))      flags |= MDB_NOTLS;
      else if (!strcmp(buf, "nolock"))     flags |= MDB_NOLOCK;
      else if (!strcmp(buf, "nordahead"))  flags |= MDB_NORDAHEAD;
      else if (!strcmp(buf, "nomeminit"))  flags |= MDB_NOMEMINIT;
      else return -1;
      res = enif_get_list_cell(env, tail, &head, &tail);
   }
   return flags;
}

//--------------------------------------------------------------------------------------------------------------------//
static int32_t read_db_flags(ErlNifEnv* env, ERL_NIF_TERM list)
{
   ERL_NIF_TERM head;
   ERL_NIF_TERM tail;
   int32_t res = enif_get_list_cell(env, list, &head, &tail);
   int32_t flags = 0;
   while(res)
   {
      char buf[64] = {};
      if (!enif_get_atom(env, head, buf, sizeof(buf), ERL_NIF_LATIN1))
      {
         return -1;
      }
      if (!strcmp(buf, "reversekey"))      flags |= MDB_REVERSEKEY;
      else if (!strcmp(buf, "dupsort"))    flags |= MDB_DUPSORT;
      else if (!strcmp(buf, "integerkey")) flags |= MDB_INTEGERKEY;
      else if (!strcmp(buf, "dupfixed"))   flags |= MDB_DUPFIXED;
      else if (!strcmp(buf, "integerdup")) flags |= MDB_INTEGERDUP;
      else if (!strcmp(buf, "reversedup")) flags |= MDB_REVERSEDUP;
      else return -1;
      res = enif_get_list_cell(env, tail, &head, &tail);
   }
   return flags;
}

//--------------------------------------------------------------------------------------------------------------------//
static int32_t read_put_flags(ErlNifEnv* env, ERL_NIF_TERM list)
{
   ERL_NIF_TERM head;
   ERL_NIF_TERM tail;
   int32_t res = enif_get_list_cell(env, list, &head, &tail);
   int32_t flags = 0;
   while(res)
   {
      char buf[64] = {};
      if (!enif_get_atom(env, head, buf, sizeof(buf), ERL_NIF_LATIN1))
      {
         return -1;
      }
      if (!strcmp(buf, "current"))          flags |= MDB_CURRENT;
      else if (!strcmp(buf, "nodupdata"))   flags |= MDB_NODUPDATA;
      else if (!strcmp(buf, "noowerwrite")) flags |= MDB_NOOVERWRITE;
      else if (!strcmp(buf, "reserve"))     flags |= MDB_RDONLY;
      else if (!strcmp(buf, "append"))      flags |= MDB_WRITEMAP;
      else if (!strcmp(buf, "appenddup"))   flags |= MDB_NOMETASYNC;
      else if (!strcmp(buf, "multiple"))    flags |= MDB_MULTIPLE;
      else return -1;
      res = enif_get_list_cell(env, tail, &head, &tail);
   }
   return flags;
}

//--------------------------------------------------------------------------------------------------------------------//
static int32_t read_cur_op_flags(ErlNifEnv* env, ERL_NIF_TERM list)
{
   ERL_NIF_TERM head;
   ERL_NIF_TERM tail;
   int32_t res = enif_get_list_cell(env, list, &head, &tail);
   int32_t flags = 0;
   while(res)
   {
      char buf[64] = {};
      if (!enif_get_atom(env, head, buf, sizeof(buf), ERL_NIF_LATIN1))
      {
         return -1;
      }
      if (!strcmp(buf, "first"))                flags |= MDB_FIRST;
      else if (!strcmp(buf, "first_dup"))       flags |= MDB_FIRST_DUP;
      else if (!strcmp(buf, "get_both"))        flags |= MDB_GET_BOTH;
      else if (!strcmp(buf, "get_both_range"))  flags |= MDB_GET_BOTH_RANGE;
      else if (!strcmp(buf, "get_current"))     flags |= MDB_GET_CURRENT;
      else if (!strcmp(buf, "get_multiple"))    flags |= MDB_GET_MULTIPLE;
      else if (!strcmp(buf, "last"))            flags |= MDB_LAST;
      else if (!strcmp(buf, "last_dup"))        flags |= MDB_LAST_DUP;
      else if (!strcmp(buf, "next"))            flags |= MDB_NEXT;
      else if (!strcmp(buf, "next_dup"))        flags |= MDB_NEXT_DUP;
      else if (!strcmp(buf, "next_nodup"))      flags |= MDB_NEXT_NODUP;
      else if (!strcmp(buf, "next_multiple"))   flags |= MDB_NEXT_MULTIPLE;
      else if (!strcmp(buf, "prev"))            flags |= MDB_PREV;
      else if (!strcmp(buf, "prev_dup"))        flags |= MDB_PREV_DUP;
      else if (!strcmp(buf, "prev_nodup"))      flags |= MDB_PREV_NODUP;
      else if (!strcmp(buf, "prev_multiple"))   flags |= MDB_PREV_MULTIPLE;
      else if (!strcmp(buf, "set"))             flags |= MDB_SET;
      else if (!strcmp(buf, "set_key"))         flags |= MDB_SET_KEY;
      else if (!strcmp(buf, "set_range"))       flags |= MDB_SET_RANGE;
      else return -1;
      res = enif_get_list_cell(env, tail, &head, &tail);
   }
   return flags;
}

//--------------------------------------------------------------------------------------------------------------------//
static int32_t read_env_config(ErlNifEnv* env, ERL_NIF_TERM config, mdb_env_options_t* options, ERL_NIF_TERM* err)
{
   ERL_NIF_TERM head;
   ERL_NIF_TERM tail;
   int32_t res = enif_get_list_cell(env, config, &head, &tail);
   if (!res)
   {
      *err = make_error(env, "options is not a list");
      return -1;
   }
   while(res)
   {
      const ERL_NIF_TERM* arr = NULL;
      int32_t arity = 0;
      check2(!enif_get_tuple(env, head, &arity, &arr), *err, "option is not a tuple");
      check2(arity != 2, *err, "wrong option arity");
      char cfg[64] = {};
      check2(!enif_get_atom(env, arr[0], cfg, sizeof(cfg), ERL_NIF_LATIN1), *err, "tuple name is not an atom");
      if (!strcmp(cfg, "env_flags"))
      {
         options->env_flags = read_env_flags(env, arr[1]);
         check2(options->env_flags == -1, *err, "unable to read environment flags");
      }
      else if (!strcmp(cfg, "db_flags"))
      {
         options->db_flags = read_db_flags(env, arr[1]);
         check2(options->db_flags == -1, *err, "unable to read db flags");
      }
      else if (!strcmp(cfg, "map_size"))
      {
         check2(!enif_get_long(env, arr[1], &options->map_size), *err, "wrong map_size");
      }
      else if (!strcmp(cfg, "max_readers"))
      {
         check2(!enif_get_int(env, arr[1], &options->max_readers), *err, "wrong max_readers");
      }
      else if (!strcmp(cfg, "path"))
      {
         check2(!enif_get_string(env, arr[1], options->path, sizeof(options->path), ERL_NIF_LATIN1),
               *err, "wrong path");
      }
      else
      {
         check2(0, *err, "unknown option %s", cfg);
      }
      res = enif_get_list_cell(env, tail, &head, &tail);
   }
   return 0;
error:
   return -1;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM db_open(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_env_res_t* env_res = (mdb_env_res_t*)enif_alloc_resource(env_res_type, sizeof(mdb_env_res_t));
   memset(env_res, 0, sizeof(mdb_env_res_t));
   int32_t res = mdb_env_create(&env_res->env);
   check(res, "unable to create environment. error = %s", mdb_strerror(res));
   if (read_env_config(env, argv[0], &env_res->options, &err) == -1)
   {
      goto error;
   }
   struct stat st = {};
   if (stat(env_res->options.path, &st) == -1)
   {
      check(mkdir(env_res->options.path, 0700) == -1, "unable to create db home. error = %s", strerror(errno));
   }
   if (env_res->options.map_size)
   {
      check(mdb_env_set_mapsize(env_res->env, env_res->options.map_size),
            "unable to set mapsize to %ld", env_res->options.map_size);
   }
   if (env_res->options.max_readers)
   {
      check(mdb_env_set_maxreaders(env_res->env, env_res->options.max_readers),
            "unable to set max_readers to %d", env_res->options.max_readers);
   }
   res = mdb_env_open(env_res->env, env_res->options.path, env_res->options.env_flags, 0664);
   check(res, "unable to open environment. error = %s", mdb_strerror(errno));

   MDB_txn* txn = NULL;
   res = mdb_txn_begin(env_res->env, NULL, 0, &txn);
   check(res, "unable to create transaction. error = %s", mdb_strerror(res));
   check(mdb_dbi_open(txn, NULL, env_res->options.db_flags, &env_res->dbi),
         "unable to create db. error = %s", mdb_strerror(res));
   mdb_txn_commit(txn);
   ERL_NIF_TERM env_term = enif_make_resource(env, env_res);
   enif_release_resource(env_res);
   return enif_make_tuple2(env, ok_atom, env_term);
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM db_close(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_env_res_t* env_res = NULL;
   check(!enif_get_resource(env, argv[0], env_res_type, (void**)&env_res), "wrong environment");
   if (!env_res->env)
   {
      return ok_atom;
   }
   mdb_env_close(env_res->env);
   env_res->env = NULL;
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM db_sync(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 2, "wrong arity");
   mdb_env_res_t* env_res = NULL;
   check(!enif_get_resource(env, argv[0], env_res_type, (void**)&env_res), "wrong environment");
   char buf[8] = {};
   check(!enif_get_atom(env, argv[1], buf, sizeof(buf), ERL_NIF_LATIN1), "wrong argument");
   int32_t force = 0;
   if (!strcmp(buf, "true"))
   {
      force = 1;
   }
   else if (!strcmp(buf, "false"))
   {
      force = 0;
   }
   else
   {
      err = make_error(env, "wrong argument");
      goto error;
   }
   int32_t res = mdb_env_sync(env_res->env, force);
   if (res)
   {
      err = make_error(env, "sync failed. error = %s", strerror(res));
      goto error;
   }
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM db_stat(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_txn_res_t* txn_res = NULL;
   check(!enif_get_resource(env, argv[0], txn_res_type, (void**)&txn_res), "wrong transaction resource");
   check(!txn_res->txn, "wrong transaction");
   MDB_stat stat= {};
   check(mdb_stat(txn_res->txn, txn_res->env->dbi, &stat), "unable to get stat");
   ERL_NIF_TERM arr[5] = {};
   arr[0] = enif_make_tuple2(env, depth_atom,          enif_make_long(env, stat.ms_depth));
   arr[1] = enif_make_tuple2(env, branch_pages_atom,   enif_make_long(env, stat.ms_branch_pages));
   arr[2] = enif_make_tuple2(env, leaf_pages_atom,     enif_make_long(env, stat.ms_leaf_pages));
   arr[3] = enif_make_tuple2(env, overflow_pages_atom, enif_make_long(env, stat.ms_overflow_pages));
   arr[4] = enif_make_tuple2(env, entries_atom,        enif_make_long(env, stat.ms_entries));
   return enif_make_tuple2(env, ok_atom, enif_make_list_from_array(env, arr, sizeof(arr)/sizeof(ERL_NIF_TERM)));
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM db_empty(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_txn_res_t* txn_res = NULL;
   check(!enif_get_resource(env, argv[0], txn_res_type, (void**)&txn_res), "wrong transaction resource");
   check(!txn_res->txn, "wrong transaction");
   mdb_drop(txn_res->txn, txn_res->env->dbi, 0);
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM txn_begin(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_env_res_t* env_res = NULL;
   check(!enif_get_resource(env, argv[0], env_res_type, (void**)&env_res), "wrong environment resource");
   mdb_txn_res_t* txn_res = (mdb_txn_res_t*)enif_alloc_resource(txn_res_type, sizeof(mdb_txn_res_t));
   memset(txn_res, 0, sizeof(mdb_txn_res_t));
   int32_t res = mdb_txn_begin(env_res->env, NULL, 0, &txn_res->txn);
   if (res)
   {
      err = make_error(env, "unable to create transaction. error = %s", mdb_strerror(res));
      goto error;
   }
   txn_res->env = env_res;
   enif_keep_resource(env_res);
   ERL_NIF_TERM txn_term = enif_make_resource(env, txn_res);
   enif_release_resource(txn_res);
   return enif_make_tuple2(env, ok_atom, txn_term);
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM txn_commit(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_txn_res_t* txn_res = NULL;
   check(!enif_get_resource(env, argv[0], txn_res_type, (void**)&txn_res), "wrong transaction resource");
   check(!txn_res->txn, "wrong transaction");
   int32_t res = mdb_txn_commit(txn_res->txn);
   if (res)
   {
      err = make_error(env, "unable to commit transaction. error = %s", mdb_strerror(res));
      goto error;
   }
   txn_res->txn = NULL;
   enif_release_resource(txn_res->env);
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM txn_abort(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_txn_res_t* txn_res = NULL;
   check(!enif_get_resource(env, argv[0], txn_res_type, (void**)&txn_res), "wrong transaction resource");
   check(!txn_res->txn, "wrong transaction");
   mdb_txn_abort(txn_res->txn);
   txn_res->txn = NULL;
   enif_release_resource(txn_res->env);
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM put(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 4, "wrong arity");
   mdb_txn_res_t* txn_res = NULL;
   check(!enif_get_resource(env, argv[0], txn_res_type, (void**)&txn_res), "wrong transaction resource");
   check(!txn_res->txn, "wrong transaction");
   ErlNifBinary key;
   check(!enif_inspect_binary(env, argv[1], &key), "key is not a binary");
   ErlNifBinary data;
   check(!enif_term_to_binary(env, argv[2], &data), "unable to convert term to binary");
   int32_t put_flags = read_put_flags(env, argv[3]);
   check(put_flags == -1, "wrong put flags");
   MDB_val mdb_key = {key.size, key.data};
   MDB_val mdb_data = {data.size, data.data};
   int32_t res = mdb_put(txn_res->txn, txn_res->env->dbi, &mdb_key, &mdb_data, put_flags);
   check(res, "put failed. error = %s", mdb_strerror(res));
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM get(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 2, "wrong arity");
   mdb_txn_res_t* txn_res = NULL;
   check(!enif_get_resource(env, argv[0], txn_res_type, (void**)&txn_res), "wrong transaction resource");
   check(!txn_res->txn, "wrong transaction");
   ErlNifBinary key;
   check(!enif_inspect_binary(env, argv[1], &key), "key is not a binary");
   MDB_val mdb_key = {key.size, key.data};
   MDB_val mdb_data;
   int32_t res = mdb_get(txn_res->txn, txn_res->env->dbi, &mdb_key, &mdb_data);
   if (res == MDB_NOTFOUND)
   {
      return enif_make_tuple2(env, err_atom, not_found_atom);
   }
   check(res, "get failed. error = %s", mdb_strerror(res));
   ERL_NIF_TERM term;
   check(!enif_binary_to_term(env, mdb_data.mv_data, mdb_data.mv_size, &term, ERL_NIF_BIN2TERM_SAFE),
         "unable to convert binary to term");
   return enif_make_tuple2(env, ok_atom, term);
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM del(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 2, "wrong arity");
   mdb_txn_res_t* txn_res = NULL;
   check(!enif_get_resource(env, argv[0], txn_res_type, (void**)&txn_res), "wrong transaction resource");
   check(!txn_res->txn, "wrong transaction");
   ErlNifBinary key;
   check(!enif_inspect_binary(env, argv[1], &key), "key is not a binary");
   MDB_val mdb_key = {key.size, key.data};
   int32_t res = mdb_del(txn_res->txn, txn_res->env->dbi, &mdb_key, NULL);
   check(res, "del failed. error = %s", mdb_strerror(res));
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM cursor_open(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_txn_res_t* txn_res = NULL;
   check(!enif_get_resource(env, argv[0], txn_res_type, (void**)&txn_res), "wrong transaction resource");
   check(!txn_res->txn, "wrong transaction");
   mdb_cur_res_t* cur_res = (mdb_cur_res_t*)enif_alloc_resource(cur_res_type, sizeof(mdb_cur_res_t));
   int32_t res = mdb_cursor_open(txn_res->txn, txn_res->env->dbi, &cur_res->cur);
   check(res, "%s", mdb_strerror(res));
   ERL_NIF_TERM cur_term = enif_make_resource(env, cur_res);
   enif_release_resource(cur_res);
   return enif_make_tuple2(env, ok_atom, cur_term);
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM cursor_close(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_cur_res_t* cur_res = NULL;
   check(!enif_get_resource(env, argv[0], cur_res_type, (void**)&cur_res), "wrong cursor resource");
   check(!cur_res->cur, "already closed");
   mdb_cursor_close(cur_res->cur);
   cur_res->cur = NULL;
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM cursor_get(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 3, "wrong arity");
   mdb_cur_res_t* cur_res = NULL;
   check(!enif_get_resource(env, argv[0], cur_res_type, (void**)&cur_res), "wrong cursor resource");
   check(!cur_res->cur, "cursor is closed");
   ErlNifBinary key;
   check(!enif_inspect_binary(env, argv[1], &key), "key is not a binary");
   int32_t cur_op_flags = read_cur_op_flags(env, argv[2]);
   check(cur_op_flags == -1, "unable to read cursor flags");
   MDB_val mdb_key = {key.size, key.data};
   MDB_val mdb_data;
   int32_t res = mdb_cursor_get(cur_res->cur, &mdb_key, &mdb_data, cur_op_flags);
   if (res == MDB_NOTFOUND)
   {
      return not_found_atom;
   }
   check(res, "cursor_get failed. error = %s", mdb_strerror(res));
   ERL_NIF_TERM term;
   check(!enif_binary_to_term(env, mdb_data.mv_data, mdb_data.mv_size, &term, ERL_NIF_BIN2TERM_SAFE),
         "unable to convert binary to term");
   return enif_make_tuple2(env, ok_atom, term);
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM cursor_put(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 4, "wrong arity");
   mdb_cur_res_t* cur_res = NULL;
   check(!enif_get_resource(env, argv[0], cur_res_type, (void**)&cur_res), "wrong cursor resource");
   check(!cur_res->cur, "cursor is closed");
   ErlNifBinary key;
   check(!enif_inspect_binary(env, argv[1], &key), "key is not a binary");
   ErlNifBinary data;
   check(!enif_term_to_binary(env, argv[2], &data), "unable to convert term to binary");
   int32_t put_flags = read_put_flags(env, argv[3]);
   check(put_flags == -1, "wrong put flags");
   MDB_val mdb_key = {key.size, key.data};
   MDB_val mdb_data = {data.size, data.data};
   int32_t res = mdb_cursor_put(cur_res->cur, &mdb_key, &mdb_data, put_flags);
   check(res, "put failed. error = %s", mdb_strerror(res));
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM cursor_del(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_cur_res_t* cur_res = NULL;
   check(!enif_get_resource(env, argv[0], cur_res_type, (void**)&cur_res), "wrong cursor resource");
   check(!cur_res->cur, "cursor is closed");
   int32_t del_flags = read_put_flags(env, argv[4]);
   check(del_flags == -1, "wrong flags");
   int32_t res = mdb_cursor_del(cur_res->cur, del_flags);
   check(res, "del failed. error = %s", mdb_strerror(res));
   return ok_atom;
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ERL_NIF_TERM cursor_count(ErlNifEnv* env, int32_t argc, const ERL_NIF_TERM argv[])
{
   ERL_NIF_TERM err;
   check(argc != 1, "wrong arity");
   mdb_cur_res_t* cur_res = NULL;
   check(!enif_get_resource(env, argv[0], cur_res_type, (void**)&cur_res), "wrong cursor resource");
   check(!cur_res->cur, "cursor is closed");
   mdb_size_t cnt = 0;
   int32_t res = mdb_cursor_count(cur_res->cur, &cnt);
   check(res, "count failed. error = %s", mdb_strerror(res));
   return enif_make_tuple2(env, ok_atom, enif_make_long(env, cnt));
error:
   return err;
}

//--------------------------------------------------------------------------------------------------------------------//
static ErlNifFunc nif_funcs[] =
{
   {"db_open",       1, db_open,      ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"db_close",      1, db_close,     ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"db_sync",       2, db_sync,      ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"db_stat",       1, db_stat,      ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"db_empty",      1, db_empty,     ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"txn_begin",     1, txn_begin,    ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"txn_commit",    1, txn_commit,   ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"txn_abort",     1, txn_abort,    ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"put",           4, put,          ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"get",           2, get,          ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"del",           2, del,          ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"cursor_open",   1, cursor_open,  ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"cursor_close",  1, cursor_close, ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"cursor_get",    3, cursor_get,   ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"cursor_put",    4, cursor_put,   ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"cursor_del",    1, cursor_del,   ERL_NIF_DIRTY_JOB_CPU_BOUND},
   {"cursor_count",  1, cursor_count, ERL_NIF_DIRTY_JOB_CPU_BOUND}
};

ERL_NIF_INIT(elmdb, nif_funcs, on_load, NULL, NULL, NULL)
