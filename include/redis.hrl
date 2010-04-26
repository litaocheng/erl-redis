%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc redis header file included by the client user
%%%
%%%----------------------------------------------------------------------
-ifndef(REDIS_HRL).
-define(REDIS_HRL, ok).

%% sort options argument
%% more explanation see redis wiki
-record(redis_sort, {
    by_pat = "" :: string(),
    get_pat = [] :: [string()],    % get pattern list
    limit = null,                  % the limit info {Start, Count}
    asc = true, 
    alpha = false,
    store = "" :: iolist() 
    }).

-type redis_sort() :: #redis_sort{}.

-endif. % REDIS_HRL
