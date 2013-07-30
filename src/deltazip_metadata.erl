-module(deltazip_metadata).

-export([read/2, pack/1]).
-export([encode_symbolic/1, decode_symbolic/1]).

-import(deltazip_iutil, [varlen_encode/1, varlen_decode/1]).

-define(TIMESTAMP_KEYTAG,  1).
-define(VERSION_ID_KEYTAG, 2).
-define(ANCESTOR_KEYTAG,   3).

-define(START_OF_YEAR_2000_IN_GREGORIAN_SECONDS,
        % = calendar:datetime_to_gregorian_seconds({{2000,1,1},{0,0,0}})
        % = (2000*365 + (2000 div 4 - 2000 div 100 + 2000 div 400)) * 24 * 60 * 60
        63113904000).

%%======================================================================

pack(Items) ->
    UnSymbolicItems = [encode_symbolic(X) || X <- Items],
    PackedItems = [ [varlen_encode(K),
                     varlen_encode(byte_size(V)),
                     V]
                    || {K,V} <- UnSymbolicItems],
    ItemsTotalSize = iolist_size(PackedItems),
    Contents0 = [varlen_encode(ItemsTotalSize) | PackedItems],

    %% Add checksum:
    Contents = iolist_to_binary(Contents0),
    [Contents, 255-compute_mod255_checksum(Contents)].

read(ReadFun, StartPos) when is_function(ReadFun,2),
                             is_integer(StartPos) ->
    VarlenReadSize = 5,
    MDSizeBin = ReadFun(StartPos, VarlenReadSize),
    {MDSize, RestAfterMDSize} = varlen_decode(MDSizeBin),
    MDSizeLen = VarlenReadSize - byte_size(RestAfterMDSize),
    %% MDSizeLen is the number of bytes in which MDSize was encoded.

    %% Read the metadata:
    <<MDBin:MDSize/binary, ExpectedCksum>> =
        ReadFun(StartPos+MDSizeLen, MDSize+1),
    TotalMDSize = MDSizeLen + MDSize + 1,
    Items = parse_metadata_items(MDBin, []),

    %% Verify checksum:
    <<MDSizeSrc:MDSizeLen/binary, _/binary>> = MDSizeBin,
    ActualCksum = compute_mod255_checksum(<<MDSizeSrc/binary, MDBin/binary, ExpectedCksum>>),
    if ActualCksum =:= 0 ->
            ok;
       true ->
            error({metadata_checksum_error, [{actual, ActualCksum},
                                             {expected, 0}]})
    end,
    {Items, TotalMDSize}.

compute_mod255_checksum(Bin) ->
    compute_mod255_checksum(Bin, 0, 0).

compute_mod255_checksum(Bin, Pos, Acc) ->
    if Pos >= byte_size(Bin) -> Acc rem 255;
       true ->
            Acc2 = Acc + binary:at(Bin,Pos),
            compute_mod255_checksum(Bin, Pos+1, Acc2)
    end.


parse_metadata_items(<<>>, Acc) ->
    lists:reverse(Acc);
parse_metadata_items(Bin, Acc) ->
    {KeyTagInt, Rest1} = varlen_decode(Bin),
    {ValueLength, Rest2} = varlen_decode(Rest1),
    <<Value:ValueLength/binary, Rest3/binary>> = Rest2,
    Item = decode_symbolic({KeyTagInt, Value}),
    parse_metadata_items(Rest3, [Item | Acc]).

%%%========================================


-spec encode_symbolic/1 :: (
                        {timestamp, calendar:datetime()} |
                        {version_id, iolist()} |
                        {ancestor, iolist()} |
                        {integer(), iolist()}) ->
                                   {integer(), binary()}.
encode_symbolic({timestamp, DateTime}) ->
    SecondsSinceY2K = calendar:datetime_to_gregorian_seconds(DateTime)
        - ?START_OF_YEAR_2000_IN_GREGORIAN_SECONDS,
    {?TIMESTAMP_KEYTAG, <<SecondsSinceY2K:32>>};
encode_symbolic({version_id, VersionID}) ->
    {?VERSION_ID_KEYTAG, iolist_to_binary(VersionID)};
encode_symbolic({ancestor, AncestorVersionID}) ->
    {?ANCESTOR_KEYTAG, iolist_to_binary(AncestorVersionID)};
encode_symbolic({KeyTag, Value}) when is_integer(KeyTag) ->
    {KeyTag, iolist_to_binary(Value)}.

-spec decode_symbolic/1 :: ({integer(), binary()}) ->
                                   {timestamp, calendar:datetime()} |
                                   {version_id, iolist()} |
                                   {ancestor, iolist()} |
                                   {integer(), iolist()}.
decode_symbolic({?TIMESTAMP_KEYTAG, <<SecondsSinceY2K:32>>}) ->
    GregSecs = SecondsSinceY2K + ?START_OF_YEAR_2000_IN_GREGORIAN_SECONDS,
    {timestamp, calendar:gregorian_seconds_to_datetime(GregSecs)};
decode_symbolic({?VERSION_ID_KEYTAG, VersionID}) ->
    {version_id, VersionID};
decode_symbolic({?ANCESTOR_KEYTAG, AncestorVersionID}) ->
    {ancestor, AncestorVersionID};
decode_symbolic({KeyTag, Value}) when is_integer(KeyTag), is_binary(Value) ->
    {KeyTag, Value}.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

symbolic_roundtrip_test() ->
    Symbolics = [{timestamp, {{2000,1,1}, {0,0,0}}},
                 {timestamp, {{2013,6,14}, {12,30,45}}},
                 {version_id, <<"Hello, World!">>},
                 {version_id, <<>>},
                 {ancestor,  <<0,1,2,64,128,192,254,255>>},
                 {100, <<0,255,"Whatever">>}],
    [?assertEqual(S, ?MODULE:decode_symbolic(?MODULE:encode_symbolic(S)))
     || S <- Symbolics].

-endif.


