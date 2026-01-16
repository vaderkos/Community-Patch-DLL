--- @alias GiVal nil | number | string | boolean | integer

--- @class GiOff : integer

--- @class GiRow
--- @field private [1] GiOff
--- @field [string] GiVal
--- @overload fun(): table<string, GiVal>
--- @overload fun(c1: string): boolean
--- @overload fun(c1: string, c2: string): boolean, boolean
--- @overload fun(c1: string, c2: string, c3: string): boolean, boolean, boolean
--- @overload fun(c1: string, c2: string, c3: string, c4: string): boolean, boolean, boolean, boolean

--- @class GiRowMeta
--- @field __index fun(_: GiRow, key: string): GiVal
--- @field __newindex fun(_: GiRow): nil
--- @field __call
--- | fun(_: GiRow): table<string, GiVal>
--- | fun(_: GiRow, c1: string): boolean
--- | fun(_: GiRow, c1: string, c2: string): boolean, boolean
--- | fun(_: GiRow, c1: string, c2: string, c3: string): boolean, boolean, boolean
--- | fun(_: GiRow, c1: string, c2: string, c3: string, c4: string): boolean, boolean, boolean, boolean

--- @class GiTbl
--- @field [string] GiRow?
--- @field [number] GiRow?
--- @operator len: integer
--- @overload fun(): (fun(): GiRow?)
--- @overload fun(str: string): (fun(): GiRow?)
--- @overload fun(str: string, ...: string | number | boolean): (fun(): GiRow?)
--- @overload fun(tbl: table<string, string | number | boolean>): (fun(): GiRow?)

--- @class GiTblMeta
--- @field tbl_name string
--- @field col_funs table<string, fun(off: integer): GiVal>
--- @field row_cnt integer
--- @field row_len integer
--- @field row_data GiVal[]
--- @field row_memo table<GiOff, GiRow>
--- @field row_meta GiRowMeta
--- @field row_nkey? string
--- @field row_skey? string
--- @field row_nidx? table<integer, integer>
--- @field row_sidx? table<string, integer>
--- @field __newindex fun(_: GiTbl): nil
--- @field __index fun(_: GiTbl, key: string | number): GiRow
--- @field __len fun(_: GiTbl): integer
--- @field __call
--- | fun(_: GiTbl): (fun(): GiRow?)
--- | fun(_: GiTbl, str: string): (fun(): GiRow?)
--- | fun(_: GiTbl, str: string, ...: string | number | boolean): (fun(): GiRow?)
--- | fun(_: GiTbl, tbl: table<string, string | number | boolean>): (fun(): GiRow?)


-- tbl_name string
-- tbl_size integer
-- tbl_data GiVal[]
-- tbl_cols table<string, GiCol>
-- tbl_rows table<string, GiVal>[]
-- tbl_nkey string
-- tbl_nmap table<integer, integer>
-- tbl_skey string
-- tbl_smap table<string, integer>
-- col_stats table<string, GiColStat>
-- col_plans table<string, GiColPlan>
-- col_readers table<string, function>
-- col_writers table<string, function>
-- row_meta metatable
-- row_memo table<integer, integer>
-- row_stride integer

