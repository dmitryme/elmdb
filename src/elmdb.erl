-module(elmdb).

-export([db_open/1,
         db_close/1,
         db_sync/2,
         db_stat/1,
         db_empty/1,
         txn_begin/1,
         txn_commit/1,
         txn_abort/1,
         put/4,
         put/3,
         get/2,
         del/2,
         cursor_open/1,
         cursor_close/1,
         cursor_get/3,
         cursor_put/4,
         cursor_del/1,
         cursor_count/1
        ]).

-on_load(init/0).

-type env_flag() :: fixed_map | nosubdir | rdonly | writemap | nometasync |
                        nosync | mapasync | notls | nolock | nordahead | nomeminit.
-type db_flag() :: reversekey | dupsort | integerkey | dupfixed | integerdup | reversedup.
-type put_flag() :: current | nodupdata | nooverwrite | reserver | append | appenddup | multiple.
-type cur_flag() :: first | first_dup | get_both | get_both_range | get_current | get_multiple |
                        last | last_dup | next | next_dup | next_multiple | next_nodup |
                        prev | prev_dup | prev_nodup | prev_multiple | set | set_key | set_range.
-type db_type() :: binary().
-type txn_type() :: binary().
-type cur_type() :: binary().
-type reason() :: string() | nif_not_loaded.
-type db_option() :: {env_flags, [env_flag()]}    |
                     {map_size, pos_integer()}    |
                     {max_readers, pos_integer()} |
                     {path, string()}             |
                     {db_flags, [db_flag()]}.
-type db_stat_item() :: {depth, pos_integer()}          |
                        {branch_pages, pos_integer()}   |
                        {leaf_pages, pos_integer()}     |
                        {overflow_pages, pos_integer()} |
                        {entries, pos_integer()}.

init() ->
   erlang:load_nif(code:priv_dir(elmdb) ++ "/elmdb", 0).

-spec db_open([db_option()]) ->  {ok, db_type()} | {error, reason()}.
db_open(_Options) ->
   {error, nif_not_loaded}.

-spec db_close(db_type()) -> ok | {error, reason()}.
db_close(_Db) ->
   {error, nif_not_loaded}.

-spec db_sync(db_type(), boolean()) -> ok | {error, reason()}.
db_sync(_Db, _Force) ->
   {error, nif_not_loaded}.

-spec db_stat(txn_type()) -> {ok, [db_stat_item()]} | {error, reason()}.
db_stat(_Txn) ->
   {error, nif_not_loaded}.

-spec db_empty(txn_type()) -> ok | {error, reason()}.
db_empty(_Txn) ->
   {error, nif_not_loaded}.

-spec txn_begin(db_type()) -> {ok, txn_type()} | {error, reason()}.
txn_begin(_Db) ->
   {error, nif_not_loaded}.

-spec txn_commit(txn_type()) -> ok | {error, reason()}.
txn_commit(_Txn) ->
   {error, nif_not_loaded}.

-spec txn_abort(txn_type()) -> ok | {error, reason()}.
txn_abort(_Txn) ->
   {error, nif_not_loaded}.

-spec put(txn_type(), binary(), binary(), [put_flag()]) -> ok | {error, reason()}.
put(_Txn, _Key, _Data, _Options) ->
   {error, nif_not_loaded}.

-spec put(db_type(), binary(), binary()) -> ok | {error, reason()}.
put(Txn, Key, Data) ->
   elmdb:put(Txn, Key, Data, []).

-spec get(txn_type(), binary()) -> {ok, term()} | not_found | {error, reason()}.
get(_Txn, _Key) ->
   {error, nif_not_loaded}.

-spec del(txn_type(), binary()) -> ok | {error, reason()}.
del(_Txn, _Key) ->
   {error, nif_not_loaded}.

-spec cursor_open(txn_type()) -> {ok, cur_type()} | {error, reason()}.
cursor_open(_Txn) ->
   {error, nif_not_loaded}.

-spec cursor_close(cur_type()) -> ok | {error, reason()}.
cursor_close(_Cur) ->
   {error, nif_not_loaded}.

-spec cursor_get(cur_type(), binary(), [cur_flag()]) -> {ok, term()} | not_found | {error, reason()}.
cursor_get(_Cur, _Key, _Options) ->
   {error, nif_not_loaded}.

-spec cursor_put(cur_type(), binary(), term(), [put_flag()]) -> ok | {error, reason()}.
cursor_put(_Cur, _Key, _Data, _Options) ->
   {error, nif_not_loaded}.

-spec cursor_del(cur_type()) -> ok | {error, reason()}.
cursor_del(_Cur) ->
   {error, nif_not_loaded}.

-spec cursor_count(cur_type()) -> ok | {error, reason()}.
cursor_count(_Cur) ->
   {error, nif_not_loaded}.
