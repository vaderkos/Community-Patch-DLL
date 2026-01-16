function TEST()
	--- @return table<string, { notnull: boolean, unique: boolean }>
	local function check_type_constraints()
		local problems = {}

		local pragma_tbl_info = 'PRAGMA table_info(`%s`)'
		local pragma_idx_list = 'PRAGMA index_list(`%s`)'
		local pragma_idx_info = 'PRAGMA index_info(`%s`)'

		local function qry(tmp, ...)
			return DB.Query(string.format(tmp, ...))
		end

		for r in DB.Query([[
		SELECT name FROM sqlite_master
		WHERE type = 'table' AND name NOT LIKE 'sqlite%'
	]]) do
			local tbl_name = r.name

			local has_id = false
			local type_col = nil

			-- 1. read columns
			for row in qry(pragma_tbl_info, tbl_name) do
				if row.name == 'ID' then
					has_id = true
				elseif row.name == 'Type' then
					type_col = {
						notnull = row.notnull == 1,
						pk = row.pk > 0,
						unique = false,
					}
				end
			end

			-- we only care about tables with BOTH ID and Type
			if has_id and type_col then
				-- PK already implies UNIQUE + NOT NULL
				if type_col.pk then
					type_col.unique = true
					type_col.notnull = true
				else
					-- 2. check unique indexes
					for idx in qry(pragma_idx_list, tbl_name) do
						if idx.unique == 1 then
							local count = 0
							local colname = nil

							for info in qry(pragma_idx_info, idx.name) do
								count = count + 1
								colname = info.name
							end

							if count == 1 and colname == 'Type' then
								type_col.unique = true
								break
							end
						end
					end
				end

				if not type_col.notnull or not type_col.unique then
					problems[tbl_name] = {
						notnull = type_col.notnull,
						unique = type_col.unique,
					}
				end
			end
		end

		return problems
	end

	local problems = check_type_constraints()

	for tbl, info in pairs(problems) do
		print(
			string.format(
				'Table %s: Type column invalid (NOT NULL=%s, UNIQUE=%s)',
				tbl,
				tostring(info.notnull),
				tostring(info.unique)
			)
		)
	end

	local collectgarbage = collectgarbage;
	local pairs = pairs;
	local unpack = unpack;
	local floor = math.floor;
	local os_clock = os.clock;

	local gc = function()
		collectgarbage("collect"); collectgarbage("collect")
	end
	local mem = function() return collectgarbage("count") * 1024 end

	local nofunc = function() end
	if not print0 then
		print0 = print;
	end
	local print0 = print0;
	local disableprint = function() print = nofunc end
	local enableprint = function() print = print0 end

	local print = print


	local stopwatch = 0;
	local function timediff()
		local now = os_clock();
		local ret = floor((now - stopwatch) * 1000);
		stopwatch = now;
		return ret;
	end

	local lastmem = 0;
	local function memdiff()
		local now = mem();
		local ret = now - lastmem;
		lastmem = now;
		return ret;
	end

	__PROTO = {};
	local __PROTO = __PROTO;

	local function getProto(what)
		local ret = {};
		for row in DB.Query("PRAGMA table_info(" .. what .. ")") do
			ret[row.name] = true;
		end
		return ret;
	end

	local function queryAllProto()
		for row in DB.Query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite%'") do
			__PROTO[row.name] = getProto(row.name);
		end
	end

	queryAllProto()

	--
	-- Part 1: load tables and iterate once
	--

	function loadAllTablesAndIterateOnce(obj, name, resetfunc, nxt, ...)
		if nxt then loadAllTablesAndIterateOnce(nxt, ...) end

		resetfunc(obj);

		local m1, t1, t2, m2, mwaste1, mwaste2;

		memdiff(gc());
		timediff();

		for tablename in pairs(__PROTO) do
			local _ = obj[tablename]
		end

		t1, m1 = timediff(), memdiff();

		mwaste1 = memdiff(gc());
		timediff();

		for tablename in pairs(__PROTO) do
			for row in obj[tablename]() do
				local _ = row;
			end
		end

		t2, m2 = timediff(), memdiff();
		mwaste2 = memdiff(gc());

		if not nxt then
			print();
			print("loadAllTablesAndIterateOnce")
			print("objname****", "t_load", "t_itx", "m_load", "gc'ed", "delta", "m_itx", "gc'ed", "delta", "total")
		end
		print(name, t1, t2, m1, mwaste1, m1 + mwaste1, m2, mwaste2, m2 + mwaste2, m1 + m2 + mwaste1 + mwaste2);
	end

	--
	-- Part 2: Validation
	--

	function validateData(obj, name, resetfunc, nxt, ...)
		if nxt then validateData(nxt, ...) end
	end

	--
	-- Part 3: Nested iteration
	--

	local iterateRowsTables = { -- Checksum = 6562600
		StringTableAll = {
			"UnitPromotions", "ID < '100'",
			"Buildings", { Cost = 200 },
			"Defines", 0,
		},
		StringAllTable = {
			"UnitPromotions", "ID < '100'",
			"Defines", 0,
			"Buildings", { Cost = 200 },
		},
		AllStringTable = {
			"Defines", 0,
			"UnitPromotions", "ID < '100'",
			"Buildings", { Cost = 200 },
		},
		AllTableString = {
			"Defines", 0,
			"Buildings", { Cost = 200 },
			"UnitPromotions", "ID < '100'",
		},
		TableAllString = {
			"Buildings", { Cost = 200 },
			"Defines", 0,
			"UnitPromotions", "ID < '100'",
		},
		TableStringAll = {
			"Buildings", { Cost = 200 },
			"UnitPromotions", "ID < '100'",
			"Defines", 0,
		},
	}

	local iterateRowsTablesSmall = { -- Checksum = 1190400
		StringTableAll = {
			"UnitPromotions", "ID < '100'",
			"Policies", { CultureCost = 10 },
			"UnitClasses", 0,
		},
		StringAllTable = {
			"UnitPromotions", "ID < '100'",
			"UnitClasses", 0,
			"Policies", { CultureCost = 10 },
		},
		AllStringTable = {
			"UnitClasses", 0,
			"UnitPromotions", "ID < '100'",
			"Policies", { CultureCost = 10 },
		},
		AllTableString = {
			"UnitClasses", 0,
			"Policies", { CultureCost = 10 },
			"UnitPromotions", "ID < '100'",
		},
		TableAllString = {
			"Policies", { CultureCost = 10 },
			"UnitClasses", 0,
			"UnitPromotions", "ID < '100'",
		},
		TableStringAll = {
			"Policies", { CultureCost = 10 },
			"UnitPromotions", "ID < '100'",
			"UnitClasses", 0,
		},
	}
	local nestedChecksum = 0;
	function nestedIterate(obj, name, cond, nxt, ...)
		for row in ((cond == 0 and obj[name]()) or obj[name](cond)) do
			if nxt then
				nestedIterate(obj, nxt, ...)
			else
				nestedChecksum = nestedChecksum + 1;
			end
		end
	end

	function iterateRows(obj, name, resetfunc, nxt, ...)
		if nxt then iterateRows(nxt, ...) end
		if not nxt then
			print();
			print("iterateRows (nested, with conditions)");
			print("objname****", "testpackage***", "check", "time", "mem", "gc'ed", "delta");
		end

		--	local testpack = iterateRowsTables ;
		local testpack = iterateRowsTablesSmall;

		if testpack == iterateRowsTables and (obj == oldGameInfo or obj == CPK.DB.Gi) then
			print(name, " - skipping test because it would take ... too long to complete");
			return;
		end

		for testid, test in pairs(testpack) do
			memdiff(gc());
			timediff();

			nestedChecksum = 0;
			nestedIterate(obj, unpack(test));

			local t1, m1, mwaste = timediff(), memdiff(), memdiff(gc());

			print(name, testid, nestedChecksum, t1, m1, mwaste, m1 + mwaste);
		end
	end

	--
	-- Part 4: Iterated nestedly, create conditions dynamically
	-- TODO: Expand further
	--

	function nestedDynamicIteration(obj, name, resetfunc, nxt, ...)
		if nxt then nestedDynamicIteration(nxt, ...) end
		if not nxt then
			print();
			print("nested dynamic iteration");
			print("objname****", "check", "time", "mem", "gc'ed", "delta");
		end

		memdiff(gc());
		timediff();

		nestedChecksum = 0;
		for building in obj.Buildings() do
			if building.PrereqTech and building.PrereqTech ~= "" then
				for unit in obj.Units("PrereqTech = '" .. building.PrereqTech .. "'") do
					for row in obj.Unit_FreePromotions({ UnitType = unit.Type }) do
						nestedChecksum = nestedChecksum + obj.UnitPromotions[row.PromotionType].DefenseMod;
					end
				end
			end
		end

		local t1, m1, mwaste = timediff(), memdiff(), memdiff(gc());

		print(name, nestedChecksum, t1, m1, mwaste, m1 + mwaste);
	end

	--
	-- Part 5: Generation of all Tooltips for Buildings/Units
	--

	function generateTooltips(obj, name, resetfunc, nxt, ...)
		if nxt then generateTooltips(nxt, ...) end

		GameInfo = obj;
		VP = nil;

		disableprint();
		include("InfoTooltipInclude");
		enableprint();

		memdiff(gc());
		timediff();

		for building in obj.Buildings() do
			GetHelpTextForBuilding(building.ID);
		end

		for unit in obj.Units() do
			GetHelpTextForUnit(unit.ID, true);
		end

		local t1, m1, mwaste = timediff(), memdiff(), memdiff(gc());

		if not nxt then
			print();
			print("Generating tooltips for Buildings and Units");
			print(
				"Memory stats given here include a lot of memory allocated for string operations - caution when making assumptions!");
			print("Main thing to look at here is time...")
			print();
			print("objname****", "time", "mem", "gc'ed", "delta");
		end
		print(name, t1, m1, mwaste, m1 + mwaste);
	end

	local function runtests0(...)
		loadAllTablesAndIterateOnce(...);

		validateData(...)

		iterateRows(...);

		nestedDynamicIteration(...);

		generateTooltips(...);
	end

	local function runtests(...)
		local __state = StateName;
		StateName = nil;

		local tmp = createGameInfoObject()
		for name in pairs(__PROTO) do
			local _ = tmp[name];
		end

		collectgarbage("stop");

		print("Running GameInfo test protocol...");


		runtests0(...);

		collectgarbage("restart");

		print();
		StateName = __state;
	end

	if not CPK then
		include("CPK.lua");
	end

	NEWGAMEINFO_NO_WARNINGS = 1 -- Would warn only about CustomModOptions string condition query when VPUI is reloaded ...
	include("newGameInfo")
	newGameInfo = createGameInfoObject()

	local resetTable = function(obj)
		-- for k in pairs(__PROTO) do
		-- 	obj[k] = nil;
		-- end
	end

	local resetOldGameInfo = function(obj)
		resetTable(debug.getmetatable(obj).__index)
	end

	local TESTOBJS = {
		oldGameInfo, "oldGameInfo", resetOldGameInfo,
		CPK.DB.Gi, "cpkGameInfo", resetTable,
		newGameInfo, "newGameInfo", resetTable,
	}

	runtests(unpack(TESTOBJS));
end
