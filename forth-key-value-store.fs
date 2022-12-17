\ key-value store for pforth
\ by gustav melck, december 2022

\ todo add restore from log function (restore last values only)

s" inc/forth-hash-files.fs" required

private{  \ {{{

: gthrow  ( ior addr u -- )
    2 pick  if  type ." ; forth-key-value-store error " dup . cr throw  else  2drop drop  then  ;

create buf 1024 chars allot
0 value blen

0 value logfid

: buf+  ( addr u -- )  >r buf blen + r@ cmove  r> blen + to blen  ;
: 0buf+  ( addr u -- )  0 to blen  buf+  ;

: log-name>buf  ( addr u -- )  0buf+ s" .log" buf+  ;

create fs 31 c, char , c,
create rs 30 c, 10 c,

: .log-fs  ( -- )  fs 2 logfid write-file s" .log-fs error 1" gthrow  ;
: .log-rs  ( -- )  rs 2 logfid write-file s" .log-rs error 1" gthrow  ;

}private

: make-kv-store  ( addr u size -- )  \ makes a hash-file, addr u, and a log file which has .log appended to the name
    -rot 2dup log-name>buf
    buf blen w/o create-file s" make-kv-store error 1" gthrow  close-file s" make-kv-store error 2" gthrow
    rot make-hash-file  ;

: open-kv-store  ( addr u -- )  \ needs to be done, even if make-kv-store has just been called
    2dup log-name>buf  open-hash-file
    buf blen r/w open-file s" open-kv-store error 1" gthrow to logfid
    logfid file-size s" open-kv-store error 2" gthrow
    logfid reposition-file s" open-kv-store error 3" gthrow  ;

: close-kv-store  ( -- )
    close-hash-file
    logfid close-file s" close-kv-store error 1" gthrow  0 to logfid  ;

: create-kv-store-vial  ( -- addr )
    2 cells allocate s" create-kv-store-vial error 1" gthrow >r
    create-hf-vial r@ !  0 r@ cell+ !  r>  ;
: free-kv-store-vial  ( addr -- )  dup @ free-hf-vial  free s" free-kv-store-vial error 1" gthrow  ;
: suspend-kv-store  ( addr -- )  dup @ suspend-hash-file  cell+ logfid swap !  ;
: resume-kv-store  ( addr -- )  dup @ resume-hash-file  cell+ @ to logfid  ;

: with-kv-store-key  ( addr u -- exists? )  2dup 0buf+  with-hf-key  ;

: kv-store!  ( val-addr val-u ref-addr ref-u -- )
    logfid file-position s" kv-store! error 1" gthrow 2>r
    logfid write-file s" kv-store! error 2" gthrow  .log-fs  \ log ref
    buf blen logfid write-file s" kv-store! error 3" gthrow .log-fs  \ log key
    2dup hf-item! hf!  \ store value
    logfid write-file s" kv-store! error 4" gthrow .log-fs  \ log value
    2r> <# #s #> logfid write-file s" kv-store! error 5" gthrow .log-rs  ;  \ log record start file position 

: kv-store@  ( addr u -- u' )  hf@ dup >r hf-item-len min r> -rot hf-item@  ;

privatize  \ }}}

\ test
\ s" gustav.hf" 64 make-kv-store
\ s" gustav.hf" open-kv-store
\ s" key0" with-kv-store-key drop
\ s" value0" s" ref0" kv-store!
\ s" key1" with-kv-store-key drop
\ s" value1" s" ref1" kv-store!
\ s" key0" with-kv-store-key . cr
\ pad 16 kv-store@ pad swap type ." ;" cr
\ s" key3" with-kv-store-key drop
\ s" value3" s" ref3" kv-store!
\ s" key3" with-kv-store-key . cr
\ pad 16 kv-store@ pad swap type ." ;" cr
\ close-kv-store

