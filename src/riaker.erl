-module(riaker).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/2, new/3, new/4
    , new_raw/3, new_raw/4
    , bucket/1
    , key/1
    , value/1
    , raw_value/1
    , delete/1, delete/2
    , get/2
    , reload/1
    , get_update_metadata/1
    , link/2
    , links/2
    , linked_object/2
    , linked_objects/2
    , put/1, put/2, put/3
    , random_key/0
    , replace_link/2, replace_link/3
    , search/2, search/3
    , signature/1
    , update_content_type/2
    , update_metadata/2
    , update_value/2, update_value/3
    , mapred/2, mapred/3, mapred/4
]).

put(Obj) ->
    ?MODULE:put(Obj, []).
    
put(Obj, Options) ->
    riakc_pb_socket:put(riak_server(), Obj, Options).
    
put(Obj, Options, Timeout) ->
    riakc_pb_socket:put(riak_server(), Obj, Options, Timeout).

get(Bucket, Key) ->
    case riakc_pb_socket:get(riak_server(), Bucket, Key) of
        {ok, Obj} ->
            Obj;
        Error = {error, _} ->
            Error
    end.

reload(RiakObject) ->
    ?MODULE:get(bucket(RiakObject), key(RiakObject)).

delete(Object) ->
    delete(bucket(Object), key(Object)).

delete(Bucket, Key) ->
    riakc_pb_socket:delete(riak_server(), Bucket, Key).

new(Bucket, Key) ->
    riakc_obj:new(Bucket, finalize_key(Key)).

new(Bucket, Key, Value) ->
    riakc_obj:new(Bucket, finalize_key(Key), encode(Value)).

new(Bucket, Key, Value, ContentType) ->
    riakc_obj:new(Bucket, finalize_key(Key), encode(Value), ContentType).

new_raw(Bucket, Key, Value) ->
    riakc_obj:new(Bucket, finalize_key(Key), Value).

new_raw(Bucket, Key, Value, ContentType) ->
	riakc_obj:new(Bucket, finalize_key(Key), Value, ContentType).

bucket(Object) ->
    riakc_obj:bucket(Object).

key(Object) ->
    riakc_obj:key(Object).

finalize_key(random) -> random_key();
finalize_key(Key) -> Key.

value(RiakObject) ->
    decode(raw_value(RiakObject)).

raw_value(RiakObject) ->
    riakc_obj:get_update_value(RiakObject).

encode(Value) -> term_to_binary(Value).
decode(Value) -> binary_to_term(Value).

random_key() ->
	ensure_crypto_server(),
	Rand = crypto:sha(term_to_binary({make_ref(), now()})),
	<<I:160/integer>> = Rand,
	list_to_binary(integer_to_list(I, 32)).

ensure_crypto_server() ->
	case whereis(crypto_server) of
		undefined ->
			crypto:start(),
			{ok, running};
		_ -> {ok, started}
	end.

get_update_metadata(Object) ->
    riakc_obj:get_update_metadata(Object).

update_value(Object, Value) ->
	riakc_obj:update_value(Object, encode(Value)).
	
update_value(Object, Value, ContentType) ->
	riakc_obj:update_value(Object, encode(Value), ContentType).

update_content_type(Object, ContentType) ->
    riakc_obj:update_content_type(Object, ContentType).

update_metadata(Object, ContentType) ->
	riakc_obj:update_metadata(Object, ContentType).

links(RiakObject) ->
    Meta = get_update_metadata(RiakObject),
    case dict:find(<<"Links">>, Meta) of
        error ->
            [];
        {ok, Links} ->
            Links
    end.

links(RiakObject, Tag) ->
    Links = links(RiakObject),
    [L || {L, Ln} <- Links, Ln =:= Tag].

link(RiakObject, Tag) ->
    Links = links(RiakObject, Tag),
    case Links of
        [] ->
            undefined;
        _ ->
        [Link | _] = Links,
        Link
    end.

linked_object(RiakObject, Linkname) -> 
    Objects = linked_objects(RiakObject, Linkname),
    case Objects of
        [H | _] ->
            H;
        [] ->
            undefined
    end.

linked_objects(RiakObject, Linkname) ->
    BucketKeyPairs = links(RiakObject, Linkname),
    [Object || Object <- [?MODULE:get(B, K) || {B, K} <- BucketKeyPairs]].

add_link(RiakObject, Link) ->
    Meta = get_update_metadata(RiakObject),
    Links = [Link | links(RiakObject)],
    MetaNew = dict:store(<<"Links">>, Links, Meta),
    update_metadata(RiakObject, MetaNew).

remove_links(RiakObject, Tag) ->
    Meta = get_update_metadata(RiakObject),
    Links = [{Link, T} || {Link, T} <- links(RiakObject), T =/= Tag],
    MetaNew = dict:store(<<"Links">>, Links, Meta),
    update_metadata(RiakObject, MetaNew).

add_link(RiakSourceObject, RiakTargetObject, Tag) ->
    add_link(RiakSourceObject, {{bucket(RiakTargetObject), key(RiakTargetObject)}, Tag}).


replace_link(RiakObject, Link) ->
    {_, Tag} = Link,
    RiakObjectLinksRemoved = remove_links(RiakObject, Tag),
    add_link(RiakObjectLinksRemoved, Link).

replace_link(RiakObject, LinkedObject, Tag) ->
    RiakObjectLinksRemoved = remove_links(RiakObject, Tag),
    add_link(RiakObjectLinksRemoved, LinkedObject, Tag).

signature(Object) ->
    {bucket(Object), key(Object), last_modified(Object)}.

last_modified(Object) ->
    Meta = get_update_metadata(Object),
    {ok, Time} = dict:find(<<"X-Riak-Last-Modified">>, Meta),
    Time.

search(Bucket, QueryField, QueryTerm) ->
    search(Bucket, QueryField ++ ":" ++ QueryTerm).

search(Bucket, Query) ->
    {ok, Pairs} = riakc_pb_socket:search(ballermann:pid(riak_client_server_pool), Bucket, Query),
    [{B, K}||[B, K] <- Pairs].

mapred(Inputs, Query) ->
    riakc_pb_socket:mapred(riak_server(), Inputs, Query).

mapred(Inputs, Query, Timeout) ->
    riakc_pb_socket:mapred(riak_server(), Inputs, Query, Timeout).

mapred(Inputs, Query, Timeout, CallTimeout) ->
    riakc_pb_socket:mapred(riak_server(), Inputs, Query, Timeout, CallTimeout).


riak_server() ->
    try
        ballermann:pid(riak_client_server_pool)
    catch
        exit:{noproc, _} -> exit(ballermann_not_running)
    end.

%% ===================================================================
%% Unit Tests
%% ===================================================================
-ifdef(TEST).

simple_test_() ->
{foreach,
fun setup_obj/0,
fun cleanup_obj/1,
[
    {"Read and write", fun check_value/0}
    , {"Add and check links", fun check_links/0}
    , {"Check if find works", fun check_find/0}
    , {"Create with key", fun creation_test_with_key_test/0}
    % , {"Create with links", fun creation_with_links_test/0}
]}.

setup_obj() ->
	application:start(sasl),
	application:start(riaker),
	delete(test_bucket(), test_key()),
	delete(<<"target_bucket">>, <<"target_key">>),
	delete(<<"target_bucket2">>, <<"target_key2">>),
	Object = new(test_bucket(), test_key(), undefined),
	ObjectCT = update_value(Object, test_data()),
	put(ObjectCT).
	
test_data() ->
	dict:from_list(
		[{"list", [
			1,2,dict:from_list([{"nested_key", <<"nested_val">>}])
			]}]).

% test_string() ->
%   "{\"list\":[1,2,{\"nested_key\":\"nested_val\"}]}".

test_bucket() ->
    <<"test_bucket">>.

test_key() ->
    <<"test_key">>.
        
cleanup_obj(_) ->
    riakc_pb_socket:delete(riak_server(), test_bucket(), test_key()).

check_value() ->
    Object = new(test_bucket(), test_key(), undefined),
    ?assertEqual(undefined, value(Object)),
    ObjectData = update_value(Object, test_data()),
    ?assertEqual(test_data(), value(ObjectData)),
    ObjectRead = reload(Object),
	?assertEqual(test_data(), value(ObjectRead)).

check_links() ->
	Object = ?MODULE:get(test_bucket(), test_key()),
	?assertEqual([], links(Object)),
	Object2 = add_link(Object, {{<<"target_bucket">>, <<"target_key">>}, <<"tag">>}),
	?assertEqual([{{<<"target_bucket">>, <<"target_key">>}, <<"tag">>}], links(Object2)),
	LinkedObject = new(<<"target_bucket2">>, <<"target_key2">>, undefined),
	LinkedObject2 = update_value(LinkedObject, test_data()),
	?MODULE:put(LinkedObject2),
	LinkedObject3 = reload(LinkedObject2),
	Object3 = add_link(Object2, LinkedObject3, <<"tag2">>),
	Links = [ {{bucket(LinkedObject3), key(LinkedObject3)}, <<"tag2">>}, {{<<"target_bucket">>, <<"target_key">>}, <<"tag">>}],
	?assertEqual(Links, links(Object3)),
	?assertEqual([{bucket(LinkedObject3), key(LinkedObject3)}], links(Object3, <<"tag2">>)),
	?assertEqual(LinkedObject3, linked_object(Object3, <<"tag2">>)),
	Object4 = remove_links(Object3, <<"tag">>),
	[FirstLink | _ ] = Links,
	?assertEqual([FirstLink], links(Object4)),
	Object5 = replace_link(Object4, {{<<"new_bucket">>, <<"new_key">>}, <<"tag2">>}),
	?assertEqual([{{<<"new_bucket">>, <<"new_key">>}, <<"tag2">>}], links(Object5)).
	
check_find() ->
	Object = ?MODULE:get(test_bucket(), test_key()),
	?assertEqual(test_bucket(), bucket(Object)),
	?assertEqual(test_key(), key(Object)),
	?assertEqual(Object, ?MODULE:get(test_bucket(), test_key())),
	?assertEqual(Object, ?MODULE:get(test_bucket(), test_key())).

random_key_test() ->
	?assert(random_key() =/= random_key()).

	
creation_test_with_key_test() ->
	Obj = new(test_bucket(), test_key(), ["a test"]),
	?MODULE:put(Obj),
	ObjGet = get(test_bucket(), test_key()),
	?assertEqual(["a test"], value(ObjGet)),
	delete(Obj).
	
% creation_with_links_test() ->
% 	Links = [{{<<"bucket1">>, <<"key1">>}, <<"tag1">>}, {{<<"bucket2">>, <<"key2">>}, <<"tag2">>}],
% 	Obj = new_with_links(test_bucket(), random, test_data(), Links),
% 	?assertEqual(test_data(), value(Obj)),
% 	?assertEqual(Links, links(Obj)),
% 	riakc_pb_socket:delete(riak_server(), test_bucket(), key(Obj)).	

% inequality_bucket_test() ->
%     O1 = new(<<"test1">>, <<"a">>, "value"),
%     O2 = new(<<"test">>, <<"a">>, "value"),
%     false = equal(O1, O2).

% bucket_test() ->
%     O = new(<<"b">>, <<"k">>),
%     ?assertEqual(<<"b">>, ?MODULE:bucket(O)).

% key_test() ->
%     O = new(<<"b">>, <<"k">>),
%     ?assertEqual(<<"k">>, ?MODULE:key(O)).

% vclock_test() ->
%     %% For internal use only
%     O = new_obj(<<"b">>, <<"k">>, <<"vclock">>, []),
%     ?assertEqual(<<"vclock">>, ?MODULE:vclock(O)).

% newcontent0_test() ->
%     O = new(<<"b">>, <<"k">>),
%     ?assertEqual(0, ?MODULE:value_count(O)),
%     ?assertEqual([], ?MODULE:get_metadatas(O)),
%     ?assertEqual([], ?MODULE:get_values(O)),
%     ?assertEqual([], ?MODULE:get_contents(O)),
%     ?assertThrow(no_metadata, get_metadata(O)),
%     ?assertThrow(no_value, get_value(O)).    

% contents0_test() ->
%     O = new_obj(<<"b">>, <<"k">>, <<"vclock">>, []),
%     ?assertEqual(0, ?MODULE:value_count(O)),
%     ?assertEqual([], ?MODULE:get_metadatas(O)),
%     ?assertEqual([], ?MODULE:get_values(O)),
%     ?assertEqual([], ?MODULE:get_contents(O)),
%     ?assertThrow(no_metadata, get_metadata(O)),
%     ?assertThrow(no_value, get_value(O)).

% contents1_test() ->
%     M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
%     O = new_obj(<<"b">>, <<"k">>, <<"vclock">>,
%                       [{M1, <<"val1">>}]),
%     ?assertEqual(1, ?MODULE:value_count(O)),
%     ?assertEqual([M1], ?MODULE:get_metadatas(O)),
%     ?assertEqual([<<"val1">>], ?MODULE:get_values(O)),
%     ?assertEqual([{M1,<<"val1">>}], ?MODULE:get_contents(O)),
%     ?assertEqual(M1, get_metadata(O)),
%     ?assertEqual(<<"val1">>, get_value(O)).

% contents2_test() ->
%     M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
%     M2 = dict:from_list([{?MD_VTAG, "tag1"}]),
%     O = new_obj(<<"b">>, <<"k">>, <<"vclock">>,
%                       [{M1, <<"val1">>},
% 											 {M2, <<"val2">>}]),
%     ?assertEqual(2, ?MODULE:value_count(O)),
%     ?assertEqual([M1, M2], ?MODULE:get_metadatas(O)),
%     ?assertEqual([<<"val1">>, <<"val2">>], ?MODULE:get_values(O)),
%     ?assertEqual([{M1,<<"val1">>},{M2,<<"val2">>}], ?MODULE:get_contents(O)),
%     ?assertThrow(siblings, get_metadata(O)),
%     ?assertThrow(siblings, get_value(O)).

% update_metadata_test() ->
%     O = new(<<"b">>, <<"k">>),
%     UM = get_update_metadata(O),
%     ?assertEqual([], dict:to_list(UM)).

% update_value_test() ->
%     O = new(<<"b">>, <<"k">>),
%     ?assertThrow(no_value, get_update_value(O)),
%     O1 = ?MODULE:update_value(O, <<"v">>),
%     ?assertEqual(<<"v">>, get_update_value(O1)),
%     M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
%     O2 = ?MODULE:update_metadata(O1, M1),
%     ?assertEqual(M1, ?MODULE:get_update_metadata(O2)).

% updatevalue_ct_test() ->
%     O = new(<<"b">>, <<"k">>),
%     ?assertThrow(no_value, get_update_value(O)),
%     O1 = update_value(O, <<"v">>, "x-application/custom"),
%     ?assertEqual(<<"v">>, get_update_value(O1)),
%     M1 = dict:from_list([{?MD_VTAG, "tag1"}]),
%     O2 = ?MODULE:update_metadata(O1, M1),
%     ?assertEqual(M1, ?MODULE:get_update_metadata(O2)),
%     ?assertEqual("x-application/custom", get_update_content_type(O1)).

% update_content_type_test() ->
%     O = new(<<"b">>, <<"k">>),
%     undefined = get_update_content_type(O),
%     O1 = update_content_type(O, "application/json"),
%     ?assertEqual("application/json", get_update_content_type(O1)).

% delete_test() ->
% 	Obj = new(<<"deleteme">>, <<"now">>, <<"nothing">>),
% 	?MODULE:put(Obj),
% 	find(<<"deleteme">>, <<"now">>),
% 	delete(Obj),
% 	?assertEqual({error, notfound}, find(<<"deleteme">>, <<"now">>)).

% get_update_data_test() ->
%     MD0 = dict:from_list([{?MD_CTYPE, "text/plain"}]),
%     MD1 = dict:from_list([{?MD_CTYPE, "application/json"}]),
%     O = new_obj(<<"b">>, <<"k">>, <<"">>,
% 								[{MD0, <<"v">>}]),
%     %% Create an updated metadata object
%     Oumd = ?MODULE:update_metadata(O, MD1),
%     ?assertEqual(MD1, get_update_metadata(Oumd)),
%     %% Create an updated value object
%     Ouv = ?MODULE:update_value(O, <<"valueonly">>),
% 		?assertEqual(<<"valueonly">>, get_update_value(Ouv)),
    
%     %% Create updated both object
%     Oboth = ?MODULE:update_value(Oumd, <<"both">>),

%     %% dbgh:start(),
%     %% dbgh:trace(?MODULE)
%     % io:format("O=~p\n", [O]),
%     ?assertEqual(<<"v">>, get_update_value(O)),
%     MD2 = get_update_metadata(O),
%     io:format("MD2=~p\n", [MD2]),
%     ?assertEqual("text/plain", md_ctype(MD2)),

%     MD3 = get_update_metadata(Oumd),
%     ?assertEqual("application/json", md_ctype(MD3)),

%     ?assertEqual(<<"valueonly">>, get_update_value(Ouv)),
%     MD4 = get_update_metadata(Ouv),
%     ?assertEqual("text/plain", md_ctype(MD4)),

%     ?assertEqual(<<"both">>, get_update_value(Oboth)),
%     MD5 = get_update_metadata(Oboth),
%     ?assertEqual("application/json", md_ctype(MD5)).
   
% get_update_data_sibs_test() ->
%     MD0 = dict:from_list([{?MD_CTYPE, "text/plain"}]),
%     MD1 = dict:from_list([{?MD_CTYPE, "application/json"}]),
%     O = new_obj(<<"b">>, <<"k">>, <<"">>, 
%                 [{MD0, <<"v">>},{MD1, <<"sibling">>}]),
%     %% Create an updated metadata object
%     Oumd = update_metadata(O, MD1),
%     %% Create an updated value object
%     Ouv = update_value(O, <<"valueonly">>),
%     %% Create updated both object
%     Oboth = update_value(Oumd, <<"both">>),
    
%     ?assertThrow({error, siblings}, get_update_value(O)),
%     ?assertThrow({error, siblings}, get_update_value(Oumd)),
%     ?assertEqual({error, siblings}, get_update_value(Ouv)),
%     ?assertEqual({ok, {MD1, <<"both">>}}, get_update_value(Oboth)).
                              
% select_sibling_test() ->
%     MD0 = dict:from_list([{?MD_CTYPE, "text/plain"}]),
%     MD1 = dict:from_list([{?MD_CTYPE, "application/json"}]),
%     O = new_obj(<<"b">>, <<"k">>, <<"">>,
%                       [{MD0, <<"sib_one">>},
%                        {MD1, <<"sib_two">>}]),
%     O1 = select_sibling(1, O),
%     O2 = select_sibling(2, O),
%     ?assertEqual("text/plain", get_update_content_type(O1)),
%     ?assertEqual(<<"sib_one">>, get_update_value(O1)),
%     ?assertEqual("application/json", get_update_content_type(O2)),
%     ?assertEqual(<<"sib_two">>, get_update_value(O2)).
   
-endif.