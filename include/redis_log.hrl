%%%----------------------------------------------------------------------
%%%
%%% @copyright erl-redis 2010
%%%
%%% @author litaocheng@gmail.com
%%% @doc the log header
%%%
%%%----------------------------------------------------------------------
-ifndef(REDIS_LOG_HRL).
-define(REDIS_LOG_HRL, ok).

-define(ML_FMT(F), " (~p:~p) "++(F)).
-define(ML_DATA(D), [?MODULE, ?LINE]++(D)).

-ifdef(NOLOG).
    -define(DEBUG(F), ok).
    -define(DEBUG2(F, D), ok).

    -define(INFO(F), ok).
    -define(INFO2(F, D), ok).

    -define(WARN(F),
        error_logger:warning_msg(?ML_FMT("[*W*]"++F), ?ML_DATA([]))).
    -define(WARN2(F, D), 
        error_logger:warning_msg(?ML_FMT("[*W*]"++F), ?ML_DATA(D))).

    -define(ERROR(F),
        error_logger:error_msg(?ML_FMT("[**E**]"++F), ?ML_DATA([]))).
    -define(ERROR2(F, D),
        error_logger:error_msg(?ML_FMT("[**E**]"++F), ?ML_DATA(D))).

-else.

    -ifdef(EUNIT).

        -define(EUNITFMT(T, F, D), (?debugFmt((T) ++ "(~p) " ++ (F), [self()] ++ (D)))).

        -define(DEBUG(F), ?EUNITFMT("[D]", F, [])).
        -define(DEBUG2(F, D), ?EUNITFMT("[D]", F, D)).

        -define(INFO(F), ?EUNITFMT("[I]", F, [])).
        -define(INFO2(F, D), ?EUNITFMT("[I]", F, D)).

        -define(WARN(F), ?EUNITFMT("[*W*]", F, [])).
        -define(WARN2(F, D), ?EUNITFMT("[*W*]", F, D)).

        -define(ERROR(F), ?EUNITFMT("[**E**]", F, [])).
        -define(ERROR2(F, D), ?EUNITFMT("[**E**]", F, D)).

    -else.
        -define(DEBUG(F), 
            error_logger:info_msg(?ML_FMT("[D]"++F), ?ML_DATA([]))).
        -define(DEBUG2(F, D), 
            error_logger:info_msg(?ML_FMT("[D]"++F), ?ML_DATA(D))).

        -define(INFO(F), 
            error_logger:info_msg(?ML_FMT("[I]"++F), ?ML_DATA([]))).
        -define(INFO2(F, D),
            error_logger:info_msg(?ML_FMT("[I]"++F), ?ML_DATA(D))).

        -define(WARN(F),
            error_logger:warning_msg(?ML_FMT("[*W*]"++F), ?ML_DATA([]))).
        -define(WARN2(F, D), 
            error_logger:warning_msg(?ML_FMT("[*W*]"++F), ?ML_DATA(D))).

        -define(ERROR(F),
            error_logger:error_msg(?ML_FMT("[**E**]"++F), ?ML_DATA([]))).
        -define(ERROR2(F, D),
            error_logger:error_msg(?ML_FMT("[**E**]"++F), ?ML_DATA(D))).

    -endif. %EUNIT

-endif. %NOLOG

-endif. % REDIS_LOG_HRL
