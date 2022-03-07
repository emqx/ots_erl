%%%-------------------------------------------------------------------
%% @doc ots_erl public API
%% @end
%%%-------------------------------------------------------------------

-module(ots_erl_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ots_erl_sup:start_link().

stop(_State) ->
    ok.
