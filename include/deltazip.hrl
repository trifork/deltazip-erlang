-ifndef(DELTAZIP_HRL_).
-define(DELTAZIP_HRL_, anything).

-record(dz_access, {
          get_size :: deltazip:get_size_fun(),
          pread    :: deltazip:pread_fun(),
          replace_tail :: deltazip:replace_tail_fun(_)
         }).

-endif.
