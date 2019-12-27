-module(elmdb_tests).

-export([]).

-include_lib("eunit/include/eunit.hrl").

db_open_test() ->
   {ok, Db} = elmdb:db_open([{path, "/tmp/db"}]),
   elmdb:db_close(Db).

test1_test() ->
   {ok, Db} = elmdb:db_open([{path, "/tmp/db"}]),
   {ok, Txn} = elmdb:txn_begin(Db),
   elmdb:put(Txn, <<1:64/integer>>, {1,2,3}, []),
   elmdb:put(Txn, <<2:64/integer>>, {1,2,3}, []),
   elmdb:put(Txn, <<3:64/integer>>, {1,2,3}, []),
   elmdb:put(Txn, <<4:64/integer>>, {1,2,3}, []),
   elmdb:txn_commit(Txn),
   {ok, Txn1} = elmdb:txn_begin(Db),
   {ok, Stat} = elmdb:db_stat(Txn1),
   ?assertEqual(proplists:get_value(entries, Stat, 0), 4),
   ok = elmdb:db_empty(Txn1),
   ok = elmdb:txn_commit(Txn1),
   {ok, Txn2} = elmdb:txn_begin(Db),
   {ok, Stat1} = elmdb:db_stat(Txn2),
   ok = elmdb:txn_commit(Txn2),
   ?assertEqual(proplists:get_value(entries, Stat1, -1), 0),
   elmdb:db_close(Db).

test2_test() ->
   {ok, Db} = elmdb:db_open([{path, "/tmp/db"}]),
   {ok, Txn} = elmdb:txn_begin(Db),
   elmdb:put(Txn, <<1:64/integer>>, {1,2,3}, []),
   elmdb:put(Txn, <<2:64/integer>>, {2,2,3}, []),
   elmdb:put(Txn, <<3:64/integer>>, {3,2,3}, []),
   elmdb:put(Txn, <<4:64/integer>>, {4,2,3}, []),
   {ok, D} = elmdb:get(Txn, <<1:64/integer>>),
   ?assertEqual({1,2,3}, D),
   {ok, D1} = elmdb:get(Txn, <<4:64/integer>>),
   ?assertEqual({4,2,3}, D1),
   ?assertEqual({error, not_found}, elmdb:get(Txn, <<5:64/integer>>)),
   elmdb:del(Txn, <<1:64/integer>>),
   ?assertEqual({error, not_found}, elmdb:get(Txn, <<1:64/integer>>)),
   elmdb:txn_abort(Txn),
   elmdb:db_close(Db).

test3_test() ->
   {ok, Db} = elmdb:db_open([{path, "/tmp/db"}]),
   {ok, Txn} = elmdb:txn_begin(Db),
   ok = elmdb:put(Txn, <<1:64/integer>>, {1,2,3}, []),
   ok = elmdb:put(Txn, <<2:64/integer>>, {2,2,3}, []),
   ok = elmdb:put(Txn, <<3:64/integer>>, {3,2,3}, []),
   ok = elmdb:put(Txn, <<4:64/integer>>, {4,2,3}, []),
   ok = elmdb:put(Txn, <<5:64/integer>>, {5,2,3}, []),
   ok = elmdb:put(Txn, <<6:64/integer>>, {6,2,3}, []),
   ok = elmdb:put(Txn, <<7:64/integer>>, {7,2,3}, []),
   ok = elmdb:put(Txn, <<8:64/integer>>, {8,2,3}, []),
   ok = elmdb:put(Txn, <<9:64/integer>>, {9,2,3}, []),
   ok = elmdb:put(Txn, <<10:64/integer>>, {10,2,3}, []),
   ok = elmdb:put(Txn, <<15:64/integer>>, {15,2,3}, []),
   ok = elmdb:put(Txn, <<16:64/integer>>, {16,2,3}, []),
   ok = elmdb:put(Txn, <<17:64/integer>>, {17,2,3}, []),
   ok = elmdb:put(Txn, <<18:64/integer>>, {18,2,3}, []),
   ok = elmdb:put(Txn, <<19:64/integer>>, {19,2,3}, []),
   {ok, Cur} = elmdb:cursor_open(Txn),
   {ok, Data} = elmdb:cursor_get(Cur, <<1:64/integer>>, [first]),
   ?assertEqual(Data, {1,2,3}),
   {ok, Data1} = elmdb:cursor_get(Cur, <<1:64/integer>>, [next]),
   ?assertEqual(Data1, {2,2,3}),
   {ok, Data2} = elmdb:cursor_get(Cur, <<1:64/integer>>, [next]),
   ?assertEqual(Data2, {3,2,3}),
   {ok, Data3} = elmdb:cursor_get(Cur, <<13:64/integer>>, [set_range]),
   ?assertEqual(Data3, {15,2,3}),
   {ok, Data4} = elmdb:cursor_get(Cur, <<13:64/integer>>, [next]),
   ?assertEqual(Data4, {16,2,3}),
   {ok, Data5} = elmdb:cursor_get(Cur, <<200:64/integer>>, [last]),
   ?assertEqual(Data5, {19,2,3}),
   {ok, Data6} = elmdb:cursor_get(Cur, <<200:64/integer>>, [prev]),
   ?assertEqual(Data6, {18,2,3}),
   {ok, Data5} = elmdb:cursor_get(Cur, <<200:64/integer>>, [next]),
   {error, not_found} = elmdb:cursor_get(Cur, <<200:64/integer>>, [next]),
   ok = elmdb:cursor_close(Cur),
   elmdb:txn_abort(Txn),
   elmdb:db_close(Db).
