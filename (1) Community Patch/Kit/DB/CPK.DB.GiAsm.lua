--------------------------------------------------------------------------------

local lua_next = next
local lua_type = type
local lua_error = error
local lua_select = select
local lua_tostring = tostring
local lua_setmetatable = setmetatable

local lua_math_floor = math.floor

local lua_string_format = string.format

local civ_db_query = DB.Query

local civ_table_fill = table.fill
local lua_table_sort = table.sort
local lua_table_insert = table.insert

--------------------------------------------------------------------------------

local Void = CPK.FP.Void
local Once = CPK.FP.Once
local Always = CPK.FP.Always
local Identity = CPK.FP.Identity

local ArgsUnpack = CPK.Args.Unpack
local ArgsEach = CPK.Args.Each
local ArgsPack = CPK.Args.Pack

local IsTable = CPK.Type.IsTable
local IsInteger = CPK.Type.IsInteger

local TableKeys = CPK.Table.Keys

local AssertError = CPK.Assert.Error
local AssertIsTable = CPK.Assert.IsTable
local AssertIsInteger = CPK.Assert.IsInteger

local StringBuilder = CPK.Util.StringBuilder

local NormalizeAffinity = CPK.DB.Sqlite.NormalizeAffinity

local BitLayoutAssign32 = CPK.Bit.Layout.Assign32

local BitsForCapacity = CPK.Bit.Math.BitsForCapacity
local BitsForRange = CPK.Bit.Math.BitsForRange

local Memoize = CPK.Cache.Memoize

--------------------------------------------------------------------------------

local bit_bor = CPK.Bit.Polyfill.bor
local bit_bnot = CPK.Bit.Polyfill.bnot
local bit_band = CPK.Bit.Polyfill.band
local bit_lshift = CPK.Bit.Polyfill.lshift
local bit_rshift = CPK.Bit.Polyfill.rshift

--------------------------------------------------------------------------------

--- @package
--- @class GiCol
--- @field name string
--- @field unique boolean
--- @field primary boolean
--- @field nullable boolean
--- @field affinity SqliteAffinity

--- @package
--- @class GiColStat
--- @field frequencies table<number | string | boolean | GiNil, integer>
--- @field cardinality integer
--- @field dominant_cnt integer
--- @field dominant_val? number | string | boolean | GiNil
--- @field affinity SqliteAffinity
--- @field min_int? integer
--- @field max_int? integer
--- @field is_pint boolean

--- @package
--- @class GiColPlan
--- @field col_enc? GiColEnc
--- @field col_pos? integer
--- @field bit_pos? integer
--- @field bit_len? integer
--- @field min_int? integer
--- @field default? GiVal
--- @field mapping? table<integer, number | string | boolean>
--- @field indexes? table<number | string | boolean, integer>

--------------------------------------------------------------------------------

--- @class GiNil
local GI_NIL = { 'NIL_SENTINEL' }

local GI_OID = '_ROWID_'

local GI_THRESHOLD = 0.95

local GI_COL_ENC_NKEY = 0
local GI_COL_ENC_BOOL = 1
local GI_COL_ENC_PINT = 2
local GI_COL_ENC_DICT = 3
local GI_COL_ENC_CONST = 4
local GI_COL_ENC_SCALAR = 5
local GI_COL_ENC_SPARSE = 6

--- @alias GiColEnc
--- | `GI_COL_ENC_NKEY`
--- | `GI_COL_ENC_BOOL`
--- | `GI_COL_ENC_PINT`
--- | `GI_COL_ENC_DICT`
--- | `GI_COL_ENC_CONST`
--- | `GI_COL_ENC_SCALAR`
--- | `GI_COL_ENC_SPARSE`

--------------------------------------------------------------------------------

local GiAsm = {}

function GiAsm.Assemble()
	--- @param ... GiVal | table<number, GiVal>
	--- @return string
	local gi_util_varghash = function(...)
		local args_size = lua_select('#', ...)
		local full_hash_sb = StringBuilder.New()

		for i = 1, args_size do
			local arg_data = lua_select(i, ...)
			local arg_type = lua_type(arg_data)

			if arg_type ~= 'table' then
				full_hash_sb:Append(
					arg_type .. '{' .. lua_tostring(arg_data) .. '}'
				)
			else
				local sub_hash_sb = StringBuilder.New()
				local sub_keys = TableKeys(arg_data)
				lua_table_sort(sub_keys)

				for j = 1, #sub_keys do
					local key = sub_keys[j]
					local val = arg_data[key]

					sub_hash_sb:Append(
						lua_tostring(key)
						.. '=' .. lua_type(val)
						.. '{' .. lua_tostring(val) .. '}'
					)
				end

				full_hash_sb:Append(sub_hash_sb:Concat(','))
			end
		end

		return full_hash_sb:Concat(',')
	end

	local gi_util_sortdict = (function()
		local ranks = {
			number = 1,
			string = 2,
			boolean = 3
		}

		local to_rank = function(val)
			if val == nil then return 0 end
			local rank = ranks[lua_type(val)]
			return rank and rank or 0
		end

		--- Sorting ensures:
		---   * Deterministic dictionary layout
		---   * Stable deduplication across tables
		---
		--- Order:
		---   * NIL < number < string < boolean
		---   * Then natural ordering inside type
		--- @param vals (number | string | boolean | nil)[]
		return function(vals)
			lua_table_sort(vals, function(a, b)
				if a == b then return false end
				local ra, rb = to_rank(a), to_rank(b)

				if ra ~= rb then return ra < rb end

				if ra == 1 then return a < b end
				if ra == 2 then return a < b end

				if ra == 3 then return (a and 1 or 0) < (b and 1 or 0) end

				return false
			end)
		end
	end)()


	local gi_util_memoize = (function()
		local cache = {}

		--- @generic Fn : fun(...: GiVal | table<number | string, GiVal>)
		--- @param impl Fn
		--- @return Fn
		return function(col_enc, impl)
			return Memoize(impl, cache, function(...)
				return gi_util_varghash(col_enc, ...)
			end)
		end
	end)()

	--------------------------------------------------------------------------------

	--- @param word integer
	--- @param pos integer
	--- @param bit 1 | 0 | boolean
	--- @return integer
	local gi_bit_field_set_bit = function(word, pos, bit)
		local mask = bit_lshift(1, pos)

		if bit == 1 or bit == true then
			return bit_bor(word, mask)
		end

		if bit == 0 or bit == false then
			return bit_band(word, bit_bnot(mask))
		end

		--- @diagnostic disable-next-line: missing-return
		AssertError(bit, '1 or 0 or boolean')
	end

	--- @param word integer
	--- @param pos integer
	--- @param len integer
	--- @param int integer
	--- @return integer
	local gi_bit_field_set_int = function(word, pos, len, int)
		local field_mask = bit_lshift(1, len) - 1
		local shift_mask = bit_lshift(field_mask, pos)

		word = bit_band(word, bit_bnot(shift_mask))

		return bit_bor(word, bit_lshift(bit_band(int, field_mask), pos))
	end

	--------------------------------------------------------------------------------

	local gi_asm_get_tbl_rows = (function()
		local tmp = 'SELECT _ROWID_ as _ROWID_, * FROM `%s` ORDER BY _ROWID_'

		--- @param tbl_name string
		--- @return integer, table<string, GiVal[]>[]
		return function(tbl_name)
			local sql = lua_string_format(tmp, tbl_name)
			local res = {}
			local cnt = 0

			for row in civ_db_query(sql) do
				cnt = cnt + 1
				lua_table_insert(res, lua_setmetatable(row, nil))
			end

			return cnt, res
		end
	end)()

	local gi_asm_get_tbl_cols = (function()
		local qry = function(tmp, ...)
			return civ_db_query(lua_string_format(tmp, ...))
		end

		local pragma_tbl_info = 'PRAGMA table_info(`%s`)'
		local pragma_idx_list = 'PRAGMA index_list(`%s`)'
		local pragma_idx_info = 'PRAGMA index_info(`%s`)'

		--- @param tbl_name string
		--- @return table<string, GiCol>
		return function(tbl_name)
			--- @type table<string, GiCol>
			local tbl_cols = {}

			for row in qry(pragma_tbl_info, tbl_name) do
				tbl_cols[row.name] = {
					name = row.name,
					unique = false,
					primary = row.pk > 0,
					nullable = (row.pk == 0) and (row.notnull == 0),
					affinity = NormalizeAffinity(row.type),
				}
			end

			for tbl_idx in qry(pragma_idx_list, tbl_name) do
				if tbl_idx.unique == 1 then
					local count = 0
					local cname = nil

					for idx_info in qry(pragma_idx_info, tbl_idx.name) do
						count = count + 1
						cname = idx_info.name
					end

					if count == 1 then
						local col = tbl_cols[cname]

						if col then
							col.unique = true
						end
					end
				end
			end

			return tbl_cols
		end
	end)()

	--- @param cols table<string, GiCol>
	--- @return string?, string?
	local gi_asm_get_tbl_keys = function(cols)
		local cnt_prim = 0
		local col_prim = nil
		local tbl_skey = nil

		for _, col in lua_next, cols do
			if col.primary then
				cnt_prim = cnt_prim + 1
				col_prim = col
			elseif col.name == 'Type'
					and col.affinity == 'string'
					and col.unique
					and not col.nullable
			then
				tbl_skey = col.name
			end
		end

		if cnt_prim ~= 1 or not col_prim then
			return GI_OID, nil
		end

		if col_prim.affinity == 'number' then
			return col_prim.name, tbl_skey
		end

		if col_prim.affinity == 'string' then
			return nil, col_prim.name
		end

		return GI_OID, nil
	end

	--- @param tbl_nkey string?
	--- @param tbl_rows table<string, GiVal>[]
	--- @return integer?
	local gi_asm_get_tbl_nadj = function(tbl_nkey, tbl_rows)
		if tbl_nkey == nil then return nil end

		local row_off = nil
		local tbl_size = #tbl_rows

		for i = 1, tbl_size do
			local col_val = tbl_rows[i][tbl_nkey]

			if not IsInteger(col_val) then
				return nil
			end

			--[[@cast col_val integer]]

			if i == 1 then
				row_off = col_val
			else
				if col_val ~= row_off + (i - 1) then
					return nil
				end
			end
		end

		return row_off
	end

	--- @param tbl_cols table<string, GiCol>
	--- @param tbl_rows table<string, GiVal>[]
	--- @return table<string, GiColStat>
	local gi_asm_get_col_stats = function(tbl_cols, tbl_rows)
		--- @type table<string, GiColStat>
		local stats = {}

		for key, col in lua_next, tbl_cols do
			stats[key] = {
				min_int = nil,
				max_int = nil,
				is_pint = true,
				affinity = col.affinity,
				cardinality = 0,
				frequencies = {},
				dominant_cnt = 0,
				dominant_val = nil,
			}
		end

		for _, row in lua_next, tbl_rows do
			for col_name in lua_next, tbl_cols do
				local col_val = row[col_name] --[[@as number | string | boolean | GiNil ]]

				if col_val == nil then col_val = GI_NIL end

				local stat = stats[col_name]
				local freq = stat.frequencies[col_val]

				if freq == nil then
					freq = 1
					stat.frequencies[col_val] = freq
					stat.cardinality = stat.cardinality + 1
				else
					freq = freq + 1
					stat.frequencies[col_val] = freq
				end

				if freq > stat.dominant_cnt then
					stat.dominant_cnt = freq
					stat.dominant_val = col_val
				end

				if stat.is_pint then
					if not IsInteger(col_val) then
						stat.is_pint = false
						stat.min_int = nil
						stat.max_int = nil
					else
						--[[@cast col_val integer]]

						if stat.min_int == nil or col_val < stat.min_int then
							stat.min_int = col_val
						end

						if stat.max_int == nil or col_val > stat.max_int then
							stat.max_int = col_val
						end
					end
				end
			end
		end

		return stats
	end

	--- @param tbl_size integer
	--- @param tbl_nkey? string
	--- @param tbl_nadj? integer
	--- @param col_stats table<string, GiColStat>
	--- @return integer, table<string, GiColPlan>
	local gi_asm_get_col_plans = function(tbl_size, tbl_nkey, tbl_nadj, col_stats)
		--- @param col_name string
		--- @param col_stat GiColStat
		--- @return GiColPlan
		local get_col_plan = function(col_name, col_stat)
			if col_name == tbl_nkey and IsInteger(tbl_nadj) then
				return { col_enc = GI_COL_ENC_NKEY }
			end

			if col_stat.cardinality == 1 then
				if col_stat.dominant_val == GI_NIL then
					return { col_enc = GI_COL_ENC_CONST }
				end

				return { col_enc = GI_COL_ENC_CONST, default = col_stat.dominant_val }
			end

			if col_stat.affinity == 'boolean' then
				return { col_enc = GI_COL_ENC_BOOL, bit_len = 1 }
			end

			if col_stat.is_pint then
				local bit_len = BitsForRange(col_stat.min_int, col_stat.max_int)

				if bit_len <= 24 then -- TODO adjust
					return {
						col_enc = GI_COL_ENC_PINT,
						bit_len = bit_len,
						min_int = col_stat.min_int,
						max_int = col_stat.max_int,
					}
				end
			end

			if col_stat.cardinality <= 255 then
				return {
					bit_len = BitsForCapacity(col_stat.cardinality + 1),
					col_enc = GI_COL_ENC_DICT,
					mapping = {},
					indexes = {},
				}
			end

			if GI_THRESHOLD < (col_stat.dominant_cnt / tbl_size) then
				if col_stat.dominant_val == GI_NIL then
					return { col_enc = GI_COL_ENC_SPARSE, mapping = {} }
				end

				return {
					col_enc = GI_COL_ENC_SPARSE,
					mapping = {},
					default = col_stat.dominant_val
				}
			end

			return { col_enc = GI_COL_ENC_SCALAR }
		end

		local col_plans = {} --- @type table<string, GiColPlan>
		local col_packs = {} --- @type GiColPlan[]

		for col_name, col_stat in lua_next, col_stats do
			local col_plan = get_col_plan(col_name, col_stat)
			local col_enc = col_plan.col_enc

			col_plans[col_name] = col_plan

			if col_enc == GI_COL_ENC_BOOL
					or col_enc == GI_COL_ENC_PINT
					or col_enc == GI_COL_ENC_DICT
			then
				lua_table_insert(col_packs, col_plan)
			end
		end

		local row_stride, _ = BitLayoutAssign32('col_pos', col_packs)

		for _, col_plan in lua_next, col_plans do
			if col_plan.col_enc == GI_COL_ENC_SCALAR then
				row_stride = row_stride + 1
				col_plan.col_pos = row_stride
			end
		end

		return row_stride, col_plans
	end

	--------------------------------------------------------------------------------

	--- @param tbl_size integer
	--- @param tbl_rows table<string, GiVal>[]
	--- @param tbl_nkey? string
	--- @param tbl_nmap? table<number, GiRow>
	--- @param tbl_nadj? integer
	--- @param tbl_skey? string
	--- @param tbl_smap? table<string, GiRow>
	--- @param col_plans table<string, GiColPlan>
	--- @param row_stride integer
	local gi_asm_fill_mappings = function(
			tbl_size,
			tbl_rows,
			tbl_nkey,
			tbl_nmap,
			tbl_nadj,
			tbl_skey,
			tbl_smap,
			col_plans,
			row_stride
	)
		local is_tbl_nmap_table = tbl_nkey and IsTable(tbl_nmap)
		local is_tbl_smap_table = tbl_skey and IsTable(tbl_smap)

		for i = 1, tbl_size do
			local row = tbl_rows[i]
			local row_off = row_stride * (i - 1)

			local new_row = { row_off }

			if is_tbl_nmap_table then
				if tbl_nadj and tbl_nadj ~= 1 then
					tbl_nmap[row[tbl_nkey] - tbl_nadj + 1] = new_row
				else
					tbl_nmap[row[tbl_nkey]] = new_row
				end
			end

			if is_tbl_smap_table then
				tbl_smap[row[tbl_skey]] = new_row
			end

			for col_name, col_plan in lua_next, col_plans do
				local col_enc = col_plan.col_enc
				local col_val = row[col_name]

				if col_val ~= nil then
					if col_enc == GI_COL_ENC_SPARSE and col_val ~= col_plan.default then
						col_plan.mapping[row_off] = col_val
					elseif col_enc == GI_COL_ENC_DICT then
						if not col_plan.indexes[col_val] then
							local map_idx = 1 + #col_plan.mapping

							col_plan.mapping[map_idx] = col_val
							col_plan.indexes[col_val] = map_idx
						end
					end
				end
			end
		end
	end

	local gi_asm_dedup_mappings = (function()
		local cache = {}

		--- @param col_plans table<string, GiColPlan>
		return function(col_plans)
			for _, col_plan in lua_next, col_plans do
				local col_enc = col_plan.col_enc

				if col_enc == GI_COL_ENC_DICT then
					col_plan.indexes = {}
					local mapping = col_plan.mapping
					--[[@cast mapping table<integer, number | string | boolean>]]
					gi_util_sortdict(mapping)

					for i = 1, #mapping do
						col_plan.indexes[mapping[i]] = i
					end

					local found = false

					for _, candidate in lua_next, cache do
						if #mapping == #candidate then
							local iseql = true

							for i = 1, #candidate do
								if mapping[i] ~= candidate[i] then
									iseql = false
									break
								end
							end

							if iseql then
								col_plan.mapping = candidate
								found = true
								break
							end
						end
					end

					if not found then
						lua_table_insert(cache, mapping)
					end
				end
			end
		end
	end)()

	--------------------------------------------------------------------------------

	--- @type table<GiColEnc, fun(
	--- 	tbl_data: GiVal[],
	--- 	col_plan: GiColPlan,
	--- 	row_off: integer,
	--- 	col_val: GiVal,
	--- ): nil>
	local gi_row_setters = {
		[GI_COL_ENC_NKEY] = Void,
		[GI_COL_ENC_CONST] = Void,
		[GI_COL_ENC_SPARSE] = Void,

		[GI_COL_ENC_SCALAR] = function(tbl_data, col_plan, row_off, col_val)
			tbl_data[row_off + col_plan.col_pos] = col_val
		end,

		[GI_COL_ENC_BOOL] = function(tbl_data, col_plan, row_off, col_val)
			local abs_pos = row_off + col_plan.col_pos
			local bit_pos = col_plan.bit_pos --[[@as integer]]
			local word = tbl_data[abs_pos] or 0 --[[@as integer]]

			if col_val == nil then col_val = false end

			--[[@cast col_val boolean]]

			tbl_data[abs_pos] = gi_bit_field_set_bit(word, bit_pos, col_val)
		end,

		[GI_COL_ENC_PINT] = function(tbl_data, col_plan, row_off, col_val)
			AssertIsInteger(col_val)

			local abs_pos = row_off + col_plan.col_pos
			local bit_pos = col_plan.bit_pos --[[@as integer]]
			local bit_len = col_plan.bit_len --[[@as integer]]
			local min_int = col_plan.min_int --[[@as integer]]
			local word = tbl_data[abs_pos] or 0 --[[@as integer]]

			tbl_data[abs_pos] = gi_bit_field_set_int(
				word,
				bit_pos,
				bit_len,
				col_val - min_int
			)
		end,

		[GI_COL_ENC_DICT] = function(tbl_data, col_plan, row_off, col_val)
			local abs_pos = row_off + col_plan.col_pos
			local bit_pos = col_plan.bit_pos --[[@as integer]]
			local bit_len = col_plan.bit_len --[[@as integer]]

			local word = tbl_data[abs_pos] or 0 --[[@as integer]]
			local didx = col_val ~= nil and col_plan.indexes[col_val] or 0

			tbl_data[abs_pos] = gi_bit_field_set_int(
				word,
				bit_pos,
				bit_len,
				didx
			)
		end,
	}

	--- @param tbl_size integer
	--- @param tbl_rows table<string, GiVal>[]
	--- @param col_plans table<string, GiColPlan>
	--- @param row_stride integer
	--- @return GiVal[]
	local gi_asm_build_tbl_data = function(
			tbl_size,
			tbl_rows,
			col_plans,
			row_stride
	)
		local tbl_data = civ_table_fill(nil, tbl_size * row_stride)

		for i = 1, tbl_size do
			local row = tbl_rows[i]
			local row_off = row_stride * (i - 1)

			for col_name, col_plan in lua_next, col_plans do
				local col_val = row[col_name]
				local col_enc = col_plan.col_enc

				gi_row_setters[col_enc](tbl_data, col_plan, row_off, col_val)
			end
		end

		return tbl_data
	end

	--------------------------------------------------------------------------------

	--- @diagnostic disable-next-line: undefined-global
	local JIT = IsTable(jit)
	local U32 = 2 ^ 32
	local POW2 = not JIT and {} or nil --[[@as table<integer, integer>]]

	if not JIT then
		local v = 1
		POW2[0] = 1

		for i = 1, 32 do
			v = v * 2
			POW2[i] = v
		end
	end

	local gi_asm_bind_getter_nkey = gi_util_memoize(
		GI_COL_ENC_NKEY,
		--- @param tbl_nadj? integer
		--- @param row_stride integer
		--- @return fun(row_off: integer, _: GiVal[]): integer
		function(tbl_nadj, row_stride)
			if tbl_nadj and tbl_nadj ~= 0 then
				if row_stride == 0 then
					return Identity
				end

				return function(row_off, _)
					return (row_off / row_stride) + tbl_nadj
				end
			end

			return function(row_off, _)
				return row_off / row_stride
			end
		end
	)

	local gi_asm_bind_getter_bool = gi_util_memoize(
		GI_COL_ENC_BOOL,
		--- @param col_pos integer
		--- @param bit_pos integer
		--- @return fun(row_off: integer, tbl_data: GiVal[]): boolean
		function(col_pos, bit_pos)
			if JIT then
				local mask = bit_lshift(1, bit_pos)

				return function(row_off, tbl_data)
					return bit_band(tbl_data[row_off + col_pos] --[[@as integer]], mask) ~= 0
				end
			end

			local div = POW2[bit_pos]

			return function(row_off, tbl_data)
				local word = tbl_data[row_off + col_pos]

				return lua_math_floor((word < 0 and word + U32 or word) / div) % 2 ~= 0
			end
		end
	)

	local gi_asm_bind_getter_pint = gi_util_memoize(
		GI_COL_ENC_PINT,
		--- @param col_pos integer
		--- @param bit_pos integer
		--- @param bit_len integer
		--- @param min_int integer
		--- @return fun(row_off: integer, tbl_data: GiVal[]): integer
		function(
				col_pos,
				bit_pos,
				bit_len,
				min_int
		)
			if JIT then
				local mask = bit_lshift(1, bit_len) - 1

				return function(row_off, tbl_data)
					return min_int + bit_band(
						bit_rshift(
							tbl_data[row_off + col_pos] --[[@as integer]],
							bit_pos
						),
						mask
					)
				end
			end

			local div = POW2[bit_pos]
			local mod = POW2[bit_len]

			return function(row_off, tbl_data)
				local word = tbl_data[row_off + col_pos]

				if word < 0 then
					word = word + U32
				end

				return min_int + (lua_math_floor(word / div) % mod)
			end
		end
	)


	local gi_asm_bind_getter_dict = gi_util_memoize(
		GI_COL_ENC_DICT,
		--- @param col_pos integer
		--- @param bit_pos integer
		--- @param bit_len integer
		--- @param mapping table<integer, GiVal>
		--- @return fun(row_off: integer, tbl_data: GiVal[]): GiVal
		function(
				col_pos,
				bit_pos,
				bit_len,
				mapping
		)
			if JIT then
				local mask = bit_lshift(1, bit_len) - 1

				return function(row_off, tbl_data)
					local didx = bit_band(
						bit_rshift(
							tbl_data[row_off + col_pos] --[[@as integer]],
							bit_pos
						),
						mask
					)

					return mapping[didx]
				end
			end

			local div = POW2[bit_pos]
			local mod = POW2[bit_len]

			return function(row_off, tbl_data)
				local word = tbl_data[row_off + col_pos]

				if word < 0 then
					word = word + U32
				end

				return mapping[lua_math_floor(word / div) % mod]
			end
		end)

	local gi_asm_bind_getter_scalar = gi_util_memoize(
		GI_COL_ENC_SCALAR,
		--- @param col_pos integer
		--- @return fun(row_off: integer, tbl_data: GiVal[]): GiVal
		function(col_pos)
			return function(row_off, tbl_data)
				return tbl_data[row_off + col_pos]
			end
		end
	)


	local gi_asm_bind_getter_sparse = gi_util_memoize(
		GI_COL_ENC_SPARSE,
		--- @param default GiVal
		--- @param mapping table<integer, GiVal>
		--- @return fun(row_off: integer, _: GiVal[]): GiVal
		function(default, mapping)
			if default == nil then
				return function(row_off, _) return mapping[row_off] end
			end

			return function(row_off, _)
				local col_val = mapping[row_off]

				if col_val == nil then return default end

				return col_val
			end
		end
	)

	local gi_asm_bind_getter_const = gi_util_memoize(
		GI_COL_ENC_CONST,
		--- @param default GiVal
		--- @return fun(_f: integer, _: GiVal[]): GiVal
		function(default)
			return Always(default)
		end
	)

	--------------------------------------------------------------------------------

	--- @param tbl_nadj? integer
	--- @param col_plans table<string, GiColPlan>
	--- @param row_stride integer
	--- @return table<string, fun(row_off: integer, tbl_data: GiVal[]): GiVal>
	local gi_asm_build_row_col_funs = function(tbl_nadj, col_plans, row_stride)
		local col_funs = {}

		for col_name, col_plan in lua_next, col_plans do
			local col_enc = col_plan.col_enc

			if col_enc == GI_COL_ENC_NKEY and IsInteger(tbl_nadj) then
				col_funs[col_name] = gi_asm_bind_getter_nkey(
					tbl_nadj,
					row_stride
				)
			elseif col_enc == GI_COL_ENC_BOOL then
				col_funs[col_name] = gi_asm_bind_getter_bool(
					col_plan.col_pos,
					col_plan.bit_pos
				)
			elseif col_enc == GI_COL_ENC_PINT then
				col_funs[col_name] = gi_asm_bind_getter_pint(
					col_plan.col_pos,
					col_plan.bit_pos,
					col_plan.bit_len,
					col_plan.min_int
				)
			elseif col_enc == GI_COL_ENC_DICT then
				col_funs[col_name] = gi_asm_bind_getter_dict(
					col_plan.col_pos,
					col_plan.bit_pos,
					col_plan.bit_len,
					col_plan.mapping
				)
			elseif col_enc == GI_COL_ENC_CONST then
				col_funs[col_name] = gi_asm_bind_getter_const(
					col_plan.default
				)
			elseif col_enc == GI_COL_ENC_SCALAR then
				col_funs[col_name] = gi_asm_bind_getter_scalar(
					col_plan.col_pos
				)
			elseif col_enc == GI_COL_ENC_SPARSE then
				col_funs[col_name] = gi_asm_bind_getter_sparse(
					col_plan.default,
					col_plan.mapping
				)
			end
		end

		return col_funs
	end

	local gi_asm_bind_row_newindex_method = Always(function()
		lua_error('GameInfo rows are read-only! '
			.. 'Call GameInfo[name][key]() to get a mutable copy.')
	end)

	local gi_asm_bind_row_index_method = (function()
		local err = 'Column "%s.%s" does not exist'

		--- @param tbl_name string
		--- @param tbl_data GiVal[]
		--- @param col_funs table<string, fun(row_off: integer, tbl_data: GiVal[]): GiVal>
		return function(tbl_name, tbl_data, col_funs)
			--- @param row GiRow
			--- @param row_key string
			--- @return GiVal
			return function(row, row_key)
				local col_fun = col_funs[row_key]

				if not col_fun then
					lua_error(lua_string_format(err, tbl_name, row_key))
				end

				return col_fun(row[1], tbl_data)
			end
		end
	end)()

	local gi_asm_bind_row_call_method = (function()
		--- @param col_name string?
		--- @param col_funs table<string, function>
		local exists = function(col_name, col_funs)
			if col_name == nil then return false end

			return col_funs[col_name] ~= nil
		end

		local gi_col_exist = function(arg_size, col_funs, ...)
			if arg_size > 4 then
				AssertError(arg_size, '<=4', 'Can check up-to 4 columns in one call!')
			end

			if arg_size == 1 then
				local col_name_a = ...
				return exists(col_name_a, col_funs)
			end

			if arg_size == 2 then
				local col_name_a, col_name_b = ...
				return exists(col_name_a, col_funs), exists(col_name_b, col_funs)
			end

			if arg_size == 3 then
				local col_name_a, col_name_b, col_name_c = ...
				return exists(col_name_a, col_funs),
						exists(col_name_b, col_funs),
						exists(col_name_c, col_funs)
			end

			local col_name_a, col_name_b, col_name_c, col_name_d = ...
			return exists(col_name_a, col_funs),
					exists(col_name_b, col_funs),
					exists(col_name_c, col_funs),
					exists(col_name_d, col_funs)
		end

		--- @param col_funs table<string, fun(row_off: integer, tbl_data: GiVal[]): GiVal>
		--- @param tbl_data GiVal[]
		return function(col_funs, tbl_data)
			--- @param row GiRow
			--- @param ... string?
			return function(row, ...)
				local row_off = row[1]
				local arg_size = lua_select('#', ...)

				if arg_size == 0 then
					local copy = {}

					for col_name, col_fun in lua_next, col_funs do
						copy[col_name] = col_fun(row_off, tbl_data)
					end

					return copy
				end

				return gi_col_exist(arg_size, col_funs, ...)
			end
		end
	end)()

	--------------------------------------------------------------------------------

	--- @param tbl_name string
	--- @param got string
	--- @param exp? string
	local gi_tbl_index_error = function(tbl_name, got, exp)
		if exp == nil then
			lua_error(
				lua_string_format(
					'GameInfo.%s does not support indexing but %s specified!',
					tbl_name,
					got
				),
				2
			)
		end

		lua_error(
			lua_string_format(
				'GameInfo.%s[%s] does not support indexing by %s!',
				tbl_name,
				exp,
				got
			),
			2
		)
	end

	local gi_asm_bind_tbl_newindex_method = Always(function()
		lua_error('GameInfo tables are read-only! '
			.. 'Rows cannot be added or replaced from Lua.')
	end)

	--- @param tbl_name string
	--- @param tbl_nkey? string
	--- @param tbl_nmap? table<number, GiRow>
	--- @param tbl_nadj? integer
	--- @param tbl_skey? string
	--- @param tbl_smap? table<string, GiRow>
	--- @return fun(tbl: GiTbl, key: number | string): GiRow
	local gi_asm_bind_tbl_index_method = function(
			tbl_name,
			tbl_nkey,
			tbl_nmap,
			tbl_nadj,
			tbl_skey,
			tbl_smap
	)
		-- Possible combinations
		-- num, str, adj -> should error on not string or number
		-- num, str, nil -> should error on not string or number
		-- oid, str, adj -> should error on not string
		-- oid, str, nil -> should error on not string
		-- oid, nil, nil -> should error on every
		-- oid, nil, adj -> should error on every
		-- nil, nil, nil -> not possible because of fallback

		-- Making everything inline for performance

		if tbl_skey == nil and (tbl_nkey == nil or tbl_nkey == GI_OID) then
			return function(_, skey_val)
				--- @diagnostic disable-next-line: missing-return
				gi_tbl_index_error(tbl_name, lua_type(skey_val))
			end
		end

		if tbl_nkey == nil or tbl_nkey == GI_OID then
			AssertIsTable(tbl_smap) --[[@cast tbl_smap table<string, GiRow> ]]

			return function(_, skey_val)
				if lua_type(skey_val) ~= 'string' then
					gi_tbl_index_error(tbl_name, lua_type(skey_val), 'string')
				end

				return tbl_smap[skey_val]
			end
		end

		if tbl_nkey ~= nil and tbl_skey == nil then
			AssertIsTable(tbl_nmap) --[[@cast tbl_nmap table<number, GiRow> ]]

			if tbl_nadj == nil or tbl_nadj == 1 then
				return function(_, nkey_val)
					if lua_type(nkey_val) == 'number' then
						if nkey_val % 1 ~= 0 then
							gi_tbl_index_error(tbl_name, 'float', 'integer')
						end

						return tbl_nmap[nkey_val]
					end

					gi_tbl_index_error(tbl_name, lua_type(nkey_val), 'number')
				end
			end

			return function(_, nkey_val)
				if lua_type(nkey_val) == 'number' then
					if nkey_val % 1 ~= 0 then
						gi_tbl_index_error(tbl_name, 'float', 'integer')
					end

					return tbl_nmap[nkey_val - tbl_nadj + 1]
				end

				gi_tbl_index_error(tbl_name, lua_type(nkey_val), 'number')
			end
		end

		if tbl_skey ~= nil and tbl_nkey == nil then
			AssertIsTable(tbl_smap) --[[@cast tbl_smap table<string, GiRow>]]

			return function(_, skey_val)
				if lua_type(skey_val) ~= 'string' then
					gi_tbl_index_error(tbl_name, lua_type(skey_val), 'string')
				end

				return tbl_smap[skey_val]
			end
		end

		AssertIsTable(tbl_nmap) --[[@cast tbl_nmap table<number, GiRow>]]
		AssertIsTable(tbl_smap) --[[@cast tbl_smap table<string, GiRow>]]

		if tbl_nadj ~= nil and tbl_nadj ~= 1 then
			return function(_, rkey_val)
				local type = lua_type(rkey_val)

				if type == 'number' then
					if rkey_val % 1 ~= 0 then
						gi_tbl_index_error(tbl_name, 'float', 'integer')
					end

					return tbl_nmap[rkey_val - tbl_nadj + 1]
				end

				if type == 'string' then
					return tbl_smap[rkey_val]
				end

				--- @diagnostic disable-next-line: missing-return
				gi_tbl_index_error(tbl_name, type, 'integer or string')
			end
		end

		return function(_, rkey_val)
			local type = lua_type(rkey_val)

			if type == 'number' then
				if rkey_val % 1 ~= 0 then
					gi_tbl_index_error(tbl_name, 'float', 'integer')
				end
				return tbl_nmap[rkey_val]
			end

			if type == 'string' then
				return tbl_smap[rkey_val]
			end

			--- @diagnostic disable-next-line: missing-return
			gi_tbl_index_error(tbl_name, type, 'integer or string')
		end
	end

	local gi_asm_bind_tbl_cond_matcher = (function()
		local ALLOWED_ROW_COND_VAL_TYPES = {
			number = true,
			string = true,
			boolean = true,
		}

		--- @param tbl_name string
		--- @param col_name string
		--- @param got string
		--- @param exp string
		local gi_tbl_cond_invalid = function(tbl_name, col_name, got, exp)
			local tmp = 'Column "%s.%s" can not be filtered by type %s'
			local msg = lua_string_format(tmp, tbl_name, col_name, got)
			AssertError(got, exp, msg)
		end

		--- @param tbl_name string
		--- @param tbl_nkey? string
		--- @param tbl_nadj? integer
		--- @param tbl_skey? string
		--- @param col_funs table<string, fun(row_off: integer, tbl_data: GiVal[]): GiVal>
		--- @param row_cond table<string, GiVal>
		return function(
				tbl_name,
				tbl_nkey,
				tbl_nadj,
				tbl_skey,
				col_funs,
				row_cond
		)
			local cond_cnt = 0
			local nkey_val = nil
			local skey_val = nil

			for col_name, exp_val in lua_next, row_cond do
				if not col_funs[col_name] then
					lua_error(lua_string_format(
						'Column "%s.%s" does not exist, but filter specifies it',
						tbl_name,
						col_name
					))
				end

				local exp_type = lua_type(exp_val)

				if not ALLOWED_ROW_COND_VAL_TYPES[exp_type] then
					gi_tbl_cond_invalid(tbl_name, col_name, exp_type, 'number or string or boolean')
				end

				cond_cnt = cond_cnt + 1

				if tbl_nkey ~= nil and col_name == tbl_nkey then
					nkey_val = exp_val

					if exp_type == 'number' then
						if exp_val % 1 ~= 0 then
							gi_tbl_cond_invalid(tbl_name, col_name, 'float', 'integer')
						end
					else
						gi_tbl_cond_invalid(tbl_name, col_name, exp_type, 'integer')
					end
				end

				if tbl_skey ~= nil and col_name == tbl_skey then
					skey_val = exp_val

					if exp_type ~= 'string' then
						gi_tbl_cond_invalid(tbl_name, col_name, exp_type, 'string')
					end
				end
			end

			if cond_cnt == 0 then
				lua_error('Empty filter specified for GameInfo.' .. tbl_name .. '({})')
			end

			if nkey_val and (tbl_nadj and tbl_nadj ~= 1) then
				nkey_val = nkey_val - tbl_nadj + 1
			end

			if cond_cnt == 1 then
				local cond_key, cond_val = lua_next(row_cond)

				return nkey_val, skey_val, function(row)
					return row[cond_key] == cond_val
				end
			end

			return nkey_val, skey_val, function(row)
				for col_name, exp_val in lua_next, row_cond do
					local val = row[col_name]
					local val_type = lua_type(val)
					local exp_type = lua_type(exp_val)

					if val_type == 'boolean' and exp_type == 'number' then
						if exp_val ~= 1 and exp_val ~= 0 then
							gi_tbl_cond_invalid(
								tbl_name,
								col_name,
								lua_tostring(exp_val),
								'1 or 0 or boolean'
							)
						end

						if val ~= (exp_val == 1) then
							return false
						end
					elseif val ~= exp_val then
						return false
					end
				end

				return true
			end
		end
	end)()

	--- @param tbl_name string
	--- @param tbl_nkey? string
	--- @param tbl_nmap? table<integer, GiRow>
	--- @param tbl_nadj? integer
	--- @param tbl_skey? string
	--- @param tbl_smap? table<string, GiRow>
	--- @param col_funs table<string, fun(row_off: integer, tbl_data: GiVal[]): GiVal>
	local gi_asm_bind_tbl_call_method = function(
			tbl_name,
			tbl_nkey,
			tbl_nmap,
			tbl_nadj,
			tbl_skey,
			tbl_smap,
			col_funs
	)
		local tmp = "SELECT `%s` as `1` FROM `%s` WHERE %s"

		if tbl_nkey then AssertIsTable(tbl_nmap) end
		if tbl_skey then AssertIsTable(tbl_smap) end

		--- @param tbl GiTbl
		--- @param row_cond table<string, GiVal>
		return function(tbl, row_cond, ...)
			local row_cond_type = lua_type(row_cond)

			if row_cond_type == 'nil' then
				local key, row, src = nil, nil, (tbl_nmap or tbl_smap)

				return function()
					key, row = lua_next(src --[[@as table<number | string, GiRow>]], key)
					return row
				end
			end

			if row_cond_type == 'table' then
				local nkey_val, skey_val, matches = gi_asm_bind_tbl_cond_matcher(
					tbl_name,
					tbl_nkey,
					tbl_nadj,
					tbl_skey,
					col_funs,
					row_cond
				)

				local row = nil
				if nkey_val then
					--[[@cast tbl_nmap table<number, GiRow> ]]
					row = tbl_nmap[nkey_val]
				elseif skey_val then
					--[[@cast tbl_smap table<string, GiRow> ]]
					row = tbl_smap[skey_val]
				end

				--[[@cast row GiRow? ]]

				if row then
					if matches(row) then
						return Once(row)
					else
						return Void
					end
				end

				local key = nil
				local src = tbl_nmap or tbl_smap --[[@as table<number | string, GiRow> ]]

				return function()
					while true do
						key, row = lua_next(src, key)

						if row == nil then return nil end

						if matches(row) then
							return row
						end
					end
				end
			end

			if row_cond_type == 'string' then
				local src = tbl_nmap or tbl_smap --[[@as table<number | string, GiRow> ]]
				local sql = lua_string_format(tmp, tbl_nkey or tbl_skey, tbl_name, row_cond)
				local nxt = civ_db_query(sql, ...)
				local row = nil

				if tbl_nkey and tbl_nadj and tbl_nadj ~= 1 then
					return function()
						repeat
							row = nxt()

							if not row then return nil end

							return src[row[1] - tbl_nadj + 1]
						until row == nil
					end
				end

				return function()
					repeat
						row = nxt()

						if not row then return nil end

						return src[row[1]]
					until row == nil
				end
			end

			lua_error('GameInfo does not specified iterator parameters')
		end
	end

	--- @type GiTbl
	local GI_EMPTY_TBL = lua_setmetatable({}, {
		__len = Always(0),
		__call = Always(Void),
		__index = Void,
		__newindex = gi_asm_bind_tbl_newindex_method(),
	})

	local gi_asm_build_tbl = function(tbl_name)
		local Info = function(...)
			if CPK.Var.VERBOSE then
				print('GiAsm for ' .. tbl_name, ...)
			end
		end

		local tbl_size, tbl_rows = gi_asm_get_tbl_rows(tbl_name)
		Info('Got tbl_size and tbl_rows')

		if tbl_size == 0 then return GI_EMPTY_TBL end

		local tbl_cols = gi_asm_get_tbl_cols(tbl_name)
		Info('Got tbl_cols')

		local tbl_nkey, tbl_skey = gi_asm_get_tbl_keys(tbl_cols)
		Info('Got tbl_nkey, tbl_skey', tbl_nkey, tbl_skey)

		local tbl_nadj = gi_asm_get_tbl_nadj(tbl_nkey, tbl_rows)
		Info('Got tbl_nadj', tbl_nadj)

		local col_stats = gi_asm_get_col_stats(tbl_cols, tbl_rows)
		Info('Got col_stats', 'N/A')

		local row_stride, col_plans = gi_asm_get_col_plans(
			tbl_size,
			tbl_nkey,
			tbl_nadj,
			col_stats
		)
		Info('Got row_width, col_plans', row_stride, 'N/A')


		local tbl_nmap = tbl_nkey and {} or nil
		local tbl_smap = tbl_skey and {} or nil
		Info('Got tbl_nmap, tbl_smap', tbl_nmap, tbl_smap)

		gi_asm_fill_mappings(
			tbl_size,
			tbl_rows,
			tbl_nkey,
			tbl_nmap,
			tbl_nadj,
			tbl_skey,
			tbl_smap,
			col_plans,
			row_stride
		)
		Info('Mappings filled')

		gi_asm_dedup_mappings(col_plans)
		Info('Mappings deduped')

		local tbl_data = gi_asm_build_tbl_data(
			tbl_size,
			tbl_rows,
			col_plans,
			row_stride
		)
		Info('Got tbl_data', tbl_data)

		local col_funs = gi_asm_build_row_col_funs(tbl_nadj, col_plans, row_stride)
		Info('Got col_funs')

		local row_call_fn = gi_asm_bind_row_call_method(col_funs, tbl_data)
		Info('Got row callfn', row_call_fn)

		local row_get_val = gi_asm_bind_row_index_method(tbl_name, tbl_data, col_funs)
		Info('Got row get val fn', row_get_val)

		local row_set_val = gi_asm_bind_row_newindex_method()
		Info('Got row set val fn', row_set_val)

		local row_meta = {
			__call = row_call_fn,
			__index = row_get_val,
			__newindex = row_set_val,
		}

		for _, row in lua_next, (tbl_nmap or tbl_smap) do
			lua_setmetatable(row, row_meta)
		end
		Info('Assigned row meta')

		local tbl_get_row = gi_asm_bind_tbl_index_method(
			tbl_name,
			tbl_nkey,
			tbl_nmap,
			tbl_nadj,
			tbl_skey,
			tbl_smap
		)
		Info('Got tbl_get_row', tbl_get_row)

		local tbl_set_row = gi_asm_bind_tbl_newindex_method()
		Info('Got tbl_set_row', tbl_set_row)

		local tbl_call_fn = gi_asm_bind_tbl_call_method(
			tbl_name,
			tbl_nkey,
			tbl_nmap,
			tbl_nadj,
			tbl_skey,
			tbl_smap,
			col_funs
		)
		Info('Got tbl_call_fn', tbl_call_fn)

		local tbl_meta = {
			__len = Always(tbl_size),
			__index = tbl_get_row,
			__newindex = tbl_set_row,
			__call = tbl_call_fn,
		}
		Info('Got tbl_meta', tbl_meta)

		local tbl = lua_setmetatable({}, tbl_meta)
		Info('Returning tbl')

		-- GI_DAT = {
		-- 	tbl_size = tbl_size,
		-- 	tbl_rows = tbl_rows,
		-- 	tbl_cols = tbl_cols,
		-- 	tbl_nkey = tbl_nkey,
		-- 	tbl_skey = tbl_skey,
		-- 	tbl_nadj = tbl_nadj,
		-- 	col_stats = col_stats,
		-- 	row_stride = row_stride,
		-- 	col_plans = col_plans,
		-- 	tbl_nmap = tbl_nmap,
		-- 	tbl_smap = tbl_smap,
		-- 	tbl_data = tbl_data,
		-- 	col_funs = col_funs,
		-- }

		return tbl
	end

	local sql = [[
		SELECT name FROM `sqlite_master`
		WHERE type = 'table' AND name NOT LIKE 'sqlite%'
	]]

	local gi = {}

	for r in civ_db_query(sql) do
		local tbl_name = r.name
		gi[tbl_name] = gi_asm_build_tbl(tbl_name)
	end

	-- GI_TBL = gi_asm_build_tbl('Specialists')

	lua_setmetatable(gi, {
		__newindex = function()
			lua_error('GameInfo tables are read-only!')
		end
	})

	return gi
end

CPK.DB.GiAsm = GiAsm
