local lua_next = next
local lua_print = print
local lua_xpcall = xpcall
local lua_tostring = tostring
local lua_os_clock = os.clock
local lua_debug_traceback = debug and debug.traceback or traceback

local civ_table_fill = table.fill
local lua_table_sort = table.sort
local lua_table_concat = table.concat

local lua_math_abs = math.abs
local lua_math_ceil = math.ceil
local lua_math_floor = math.floor

local lua_string_rep = string.rep
local lua_string_format = string.format
local lua_collectgarbage = collectgarbage

local now = lua_os_clock

--------------------------------------------------------------------------------

local Bind = CPK.FP.Bind

local AssertError = CPK.Assert.Error
local AssertIsTable = CPK.Assert.IsTable
local AssertIsInteger = CPK.Assert.IsInteger

local StringBuilder = CPK.Util.StringBuilder

--------------------------------------------------------------------------------

local gc_start = Bind(lua_collectgarbage, 'restart')
local gc_pause = Bind(lua_collectgarbage, 'stop')

local gc_count = function()
	return 1024 * lua_collectgarbage('count')
end

local gc_run = function(n)
	n = n or 3
	for i = 1, n do
		lua_collectgarbage('collect')
	end

	return gc_count()
end

--------------------------------------------------------------------------------

--- @param bytes number
--- @param digits? number
--- @return string
local format_mem = function(bytes, digits)
	digits = digits or 2
	local abs = lua_math_abs(bytes)
	local tmp = nil
	local val = nil

	if abs < 1024 then
		tmp, val = '%d B', bytes
	elseif abs < 1024 ^ 2 then
		tmp, val = ('%.' .. digits .. 'f KiB'), (bytes / 1024)
	elseif abs < 1024 ^ 3 then
		tmp, val = ('%.' .. digits .. 'f MiB'), (bytes / (1024 ^ 2))
	else
		tmp, val = ('%.' .. digits .. 'f GiB'), (bytes / (1024 ^ 3))
	end

	return lua_string_format(tmp, val)
end

local format_dur = function(dur, digits)
	digits = digits or 2

	if dur == 0 then
		return '0'
	end

	local abs = lua_math_abs(dur)
	local val, unit

	if abs >= 1e-3 then
		val, unit = (dur * 1e3), 'ms'
	elseif abs >= 1e-6 then
		val, unit = (dur * 1e6), 'us'
	else
		val, unit = (dur * 1e9), 'ns'
	end

	return lua_string_format('%.' .. digits .. 'f %s', val, unit)
end

--- @param s string
--- @param w integer
--- @return string
local pad_start = function(s, w)
	s = lua_tostring(s)
	local l = #s

	if l >= w then
		return s
	end

	return lua_string_rep(' ', w - l) .. s
end

--------------------------------------------------------------------------------

--- @param n integer # Samples
--- @param b integer # Batch size
--- @param f function # Function to benchmark
--- @return { left: number[], durs: number[], mems: number[] }
local bench = function(n, b, f)
	local time_prior, heap_prior, time_after, heap_after, left_prior, left_after

	local size = n / b
	local left = nil
	local durs = civ_table_fill(nil, size) -- execution durations
	local mems = civ_table_fill(nil, size) -- allocation pressure

	left_prior = gc_run()

	gc_pause()
	for i = 1, size do
		time_prior, heap_prior = now(), gc_count()
		for _ = 1, b do
			f()
		end
		time_after, heap_after = now(), gc_count()

		durs[i] = (time_after - time_prior) / b
		mems[i] = (heap_after - heap_prior) / b
	end
	gc_start()

	left_after = gc_run()

	left = left_after - left_prior

	lua_table_sort(durs)
	lua_table_sort(mems)

	return {
		left = left,
		durs = durs,
		mems = mems,
	}
end

--- @param arr number[]
--- @return number # Minimum
--- @return number # Maximum
--- @return number # Average
--- @return number # Median
local stats = function(arr)
	AssertIsTable(arr)

	local len = #arr
	if len < 1 then
		AssertError(len, '>0')
	end

	if len == 1 then
		return arr[1], arr[1], arr[1], arr[1]
	end

	local sum = 0
	for i = 1, len do
		sum = sum + arr[i]
	end

	local min = arr[1]
	local max = arr[len]
	local avg = sum / len
	local med = nil

	if len % 2 == 1 then
		med = arr[(len + 1) / 2]
	else
		local idx = lua_math_floor((len + 1) / 2)
		med = (arr[idx] + arr[idx + 1]) * 0.5
	end

	return min, max, avg, med
end

--- @class GiTestOpts
--- @field times? integer
--- @field tests table<string, fun(tbl: GiTbl): function>
--- @field cases table<string, { delete: fun(tbl: GiTbl), create: fun(): GiTbl }>

--- @param opts GiTestOpts
local function GiTest(opts)
	AssertIsTable(opts)

	local tests = opts.tests
	local cases = opts.cases
	local times = opts.times or (3 * 1000)

	AssertIsTable(tests)
	AssertIsTable(cases)
	AssertIsInteger(times)

	if times % 100 ~= 0 then
		AssertError(times, 'divisible by 100')
	end

	local results = {}

	local na = function()
		return { min = 'N/A', max = 'N/A', avg = 'N/A', med = 'N/A' }
	end

	for case_name, _ in lua_next, cases do
		results[case_name] = {}

		for test_name, _ in lua_next, tests do
			results[case_name][test_name] = {
				dur = na(),
				mem = na(),
				ret = 'N/A',
			}
		end
	end

	gc_run()

	for case_name, case_data in lua_next, cases do
		local tbl = case_data.create()

		for test_name, test_call in lua_next, tests do
			local test_info = results[case_name][test_name]
			local test_test = test_call(tbl)

			local succ, err = lua_xpcall(test_test, lua_debug_traceback)

			if succ then
				local ben_dat = bench(times, 100, test_test)

				local dur_min, dur_max, dur_avg, dur_med = stats(ben_dat.durs)
				local mem_min, mem_max, mem_avg, mem_med = stats(ben_dat.mems)

				test_info.dur.min = dur_min
				test_info.dur.max = dur_max
				test_info.dur.avg = dur_avg
				test_info.dur.med = dur_med

				test_info.mem.min = mem_min
				test_info.mem.max = mem_max
				test_info.mem.avg = mem_avg
				test_info.mem.med = mem_med

				test_info.ret = ben_dat.left
			else
				lua_print('Skipping', case_name, test_name, err)
			end

			gc_run()
		end

		case_data.delete(tbl)
		gc_run()
	end

	gc_run()

	local headers = {
		'Spec', 'Case',
		'Duration(AVG)', 'Duration(Med)', 'Duration(min)', 'Duration(max)',
		'Allocated(AVG)', 'Allocated(Med)', 'Allocated(min)', 'Allocated(max)',
		'Retained Memory',
	}

	local rows = {}
	local wids = {}

	-- init widths from header
	for i = 1, #headers do
		wids[i] = #headers[i]
	end

	for spec_name, _ in lua_next, tests do
		for case_name, case_results in lua_next, results do
			local data = case_results[spec_name]

			local row = {
				spec_name,
				case_name,
				format_dur(data.dur.avg),
				format_dur(data.dur.med),
				format_dur(data.dur.min),
				format_dur(data.dur.max),

				format_mem(data.mem.avg),
				format_mem(data.mem.med),
				format_mem(data.mem.min),
				format_mem(data.mem.max),

				format_mem(data.ret),
			}

			rows[#rows + 1] = row

			for i = 1, #row do
				local l = #lua_tostring(row[i])
				if l > wids[i] then
					wids[i] = l
				end
			end
		end
	end

	local res = StringBuilder.New()

	local sep = (function()
		local sb = StringBuilder.New()

		for i = 1, #wids do
			sb:Append(lua_string_rep('-', wids[i]))
		end

		return sb:Concat('-+-')
	end)()

	local top = (function()
		local sb = StringBuilder.New()

		for i = 1, #headers do
			sb:Append(pad_start(headers[i], wids[i]))
		end

		return sb:Concat(' | ')
	end)()

	res:Append(sep):Append(top):Append(sep)

	local last_test_name = nil
	for i = 1, #rows do
		local row = rows[i]
		local test_name = row[1]

		if last_test_name ~= nil and test_name ~= last_test_name then
			res:Append(sep)
		end

		local sb = StringBuilder.New()

		for j = 1, #row do
			sb:Append(pad_start(row[j], wids[j]))
		end

		res:Append(sb:Concat(' | '))
		last_test_name = test_name
	end

	lua_print(lua_os_clock(), '\n' .. res:Append(sep):Concat('\n'))
end

CPK.DB.GiTest = GiTest

GI_TEST_RUN = function()
	GiTest({
		tests = {
			['Policies()'] = function(tbl)
				return function()
					for row in tbl.Policies() do end
				end
			end,
			['Policies("ID < 100")'] = function(tbl)
				return function()
					for row in tbl.Policies('ID < 100') do end
				end
			end,
			['Policies("PortraitIndex > 50")'] = function(tbl)
				return function()
					for row in tbl.Policies('PortraitIndex > 50') do end
				end
			end,
			['Policies({ CultureCost = 10 })'] = function(tbl)
				return function()
					for row in tbl.Policies({ CultureCost = 10 }) do end
				end
			end,
			['Policies[0].Type'] = function(tbl)
				local policy = tbl.Policies[0]

				return function()
					return policy.Type
				end
			end,
			['Buildings("IconAtlas = \'BW_ATLAS_1\'")'] = function(tbl)
				return function()
					for row in tbl.Buildings("IconAtlas = 'BW_ATLAS_1'") do end
				end
			end,
			['Buildings({ IsCorporation = 1 , GoldMaintenance = 1 })'] = function(tbl)
				return function()
					for row in tbl.Buildings({ IsCorporation = 1, GoldMaintenance = 1 }) do end
				end
			end,
			['Buildings("ID IN (1, 2, 3, 4, 5, 6, 7, 8)")'] = function(tbl)
				return function()
					for row in tbl.Buildings('ID IN (1, 2, 3, 4, 5, 6, 7, 8)') do end
				end
			end,
			['Buildings["BUILDING_GRANARY"]'] = function(tbl)
				return function() return tbl["BUILDING_GRANARY"] end
			end,
			['Nested iteration'] = function(tbl)
				return function()
					for b in tbl.Buildings() do
						if b.PrereqTech and b.PrereqTech ~= "" then
							for u in tbl.Units({ PrereqTech = b.PrereqTech }) do
								for r in tbl.Unit_FreePromotions({ UnitType = u.Type }) do
									--
								end
							end
						end
					end
				end
			end,
			['I wanna fail'] = function()
				return function() error('I failed') end
			end,
		},
		cases = {
			-- Default GameInfo gets out of memory
			-- GameInfo = {
			-- 	create = function()
			-- 		local tbl = GameInfo

			-- 		local sql = [[
			-- 			SELECT name FROM `sqlite_master`
			-- 			WHERE type = 'table' AND name NOT LIKE 'sqlite%'
			-- 		]]

			-- 		for row in DB.Query(sql) do
			-- 			for _ in tbl[row.name]() do end
			-- 		end

			-- 		return tbl --[[@as GiTbl]]
			-- 	end,
			-- 	delete = function(tbl)
			-- 		for name in lua_next, tbl do
			-- 			tbl[name] = nil
			-- 		end
			-- 	end
			-- },
			Gi = {
				create = function()
					return CPK.DB.GiAsm.Assemble()
				end,
				delete = function(tbl)
					return
				end
			}
		}
	})
end
