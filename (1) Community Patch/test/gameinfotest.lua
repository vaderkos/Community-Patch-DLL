local collectgarbage = collectgarbage;
local pairs = pairs;
local ipairs = ipairs;
local loadstring = loadstring;
local compile = function(str) return loadstring("return " .. str)() end
local unpack = unpack;
local floor = math.floor;
local f = floor;
local os_clock = os.clock;

local CLEANUP = 7;

local gc = function()
	collectgarbage("collect"); collectgarbage("collect")
end
local mem = function() return collectgarbage("count") * 1024 end

local nofunc = function() end
if not print0 then
	print0 = print;
end

local printenabled = true;
local print0 = print0;
local toggledprint = function(...) if printenabled then print0(...) end end
local disableprint = function()
	print = toggledprint; printenabled = false
end
local enableprint = function()
	print = print0; printenabled = true
end

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
		ret[row.name] = { row.pk or -9999 }
	end
	return ret;
end

local function queryAllProto()
	for row in DB.Query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite%'") do
		__PROTO[row.name] = getProto(row.name);
	end

	--[[
	for tab, proto in pairs(__PROTO) do
		if proto.ID and proto.Type and proto.ID[1] == 1 and proto.Type[1] == 0 then
			print(proto.ID and proto.ID[1],proto.Type and proto.Type[1],proto.Name and proto.Name[1],tab);
		end
	end
	for tab, proto in pairs(__PROTO) do
		if ( proto.ID or proto.Type or proto.Name ) and not ( proto.ID and proto.Type and proto.ID[1] == 1 and proto.Type[1] == 0 ) then
			print(proto.ID and proto.ID[1],proto.Type and proto.Type[1],proto.Name and proto.Name[1],tab);
		end
	end
	--]]
end

queryAllProto()

--
-- Part 1: load tables and iterate once
--

local function loadAllTablesAndIterateOnce(obj, name, resetfunc, nxt, ...)
	if nxt then loadAllTablesAndIterateOnce(nxt, ...) end

	obj("reset");

	local m1, t1, t2, m2, mwaste1, mwaste2;

	memdiff(gc());
	timediff();

	obj = obj("load");

	t1, m1 = timediff(), memdiff();

	mwaste1 = memdiff(gc());
	timediff();

	for tablename in pairs(__PROTO) do
		--print(tablename);
		for row in obj[tablename]() do
			local _ = row;
		end
	end

	t2, m2 = timediff(), memdiff();
	mwaste2 = memdiff(gc());

	if not nxt then
		print();
		print();
		print("loadAllTablesAndIterateOnce");
		print();
		print("objname****", "t_load", "t_itx", "m_load", "gc'ed", "delta", "m_itx", "gc'ed", "delta", "|", "time_total",
			"mem_total");
		print();
	end
	print(name, t1, t2, m1, mwaste1, m1 + mwaste1, m2, mwaste2, m2 + mwaste2, "|", t1 + t2, m1 + m2 + mwaste1 + mwaste2);
end

--
-- Part 2: Validation
--

local function validateData(obj, name, resetfunc, nxt, ...)
	--if nxt then validateData(nxt,...)end
end


--
-- Part 3: Test iteration
--

local iterationAndConditionTests = {
	{
		tablename = "Buildings",
		columns = [[ { "Type", "ID", "GoldMaintenance", "IconAtlas", "Help" } ]],
		conditions = {
			[[ 'IconAtlas = "BW_ATLAS_1"' ]],
			[[ {IconAtlas = 'BW_ATLAS_1'} ]],
			[[ 'GoldMaintenance = 1' ]],
			[[ {GoldMaintenance = 1} ]],
			[[ 'IsCorporation = 1' ]],
			[[ { IsCorporation = 1 } ]],
			[[ { IsCorporation = true } ]],
			[[ 'IsCorporation = 1 AND GoldMaintenance = 1' ]],
			[[ { IsCorporation = 1 , GoldMaintenance = 1 } ]],
			[[ 0 ]],
		},
	},
}

for _, tab in ipairs(iterationAndConditionTests) do
	local conds = {};
	tab.compiled_conditions = conds;
	tab.compiled_columns = compile(tab.columns)
	for idx, cond in ipairs(tab.conditions) do
		conds[idx] = compile(cond);
	end
end

local PASSES = {
	1,
	10000,
}
local function testIterator(tablename, pass, REPEAT_NUM, condition, obj, objname, resetfunc, nxt, ...)
	if nxt then testIterator(tablename, pass, REPEAT_NUM, condition, nxt, ...) end
	obj = obj("get")

	local rows = 0;

	local a, b, c;

	local countdown = CLEANUP;
	local it, im, ig = 0, 0, 0;


	timediff(memdiff(gc()));

	for itx = 1, REPEAT_NUM do
		if condition == 0 then
			a, b, c = obj[tablename]();
		else
			a, b, c = obj[tablename](condition);
		end

		for row in a, b, c do
			rows = rows + 1;
		end

		countdown = countdown - 1
		if countdown == 0 then
			it = it + timediff()
			im, ig = im + memdiff(), ig + memdiff(gc());
			countdown = CLEANUP;
			timediff();
		end
	end


	it = it + timediff();
	im, ig = im + memdiff(), ig + memdiff(gc());

	local itt = (1000 * it) / REPEAT_NUM

	local memdiv = f((im + ig) / REPEAT_NUM)
	local igdiv = f(ig / REPEAT_NUM)

	rows = rows / (REPEAT_NUM);
	local trow = f(1000 * itt / rows);
	it = f(itt);

	--	print(	"call",	"",	"objname****",	"#",	"ttot",	"t/it",	"#rows",	"time/row",	"memtot",	"gctot",	"mem/it",	"gc/it")
	print("", "", objname, pass, it, itt, rows, trow, im + ig, ig, memdiv, igdiv);
end

local function testIterators(...)
	print();
	print();
	print("testIterators")
	print(
		"Note: This was supposed to measure time and memory for iterator generation and actual iteration separately, but lua's clock's resolution won't really allow that")
	print();
	print("PASS", "REPEATS")
	for pass, REPEAT_NUM in ipairs(PASSES) do
		print(pass, REPEAT_NUM);
	end
	print();
	print("call", "", "objname****", "#PASS", "ttot", "t/it", "#rows", "t/row", "mtot", "gctot", "mem/it", "gc/it")
	print();
	for _, tab in ipairs(iterationAndConditionTests) do
		for itx, conditionstr in ipairs(tab.conditions) do
			print(tab.tablename .. "(" .. conditionstr .. ")");
			for pass, REPEAT_NUM in ipairs(PASSES) do
				testIterator(tab.tablename, pass, REPEAT_NUM, tab.compiled_conditions[itx], ...)
			end
		end
	end
end

--
-- Part 4: Generation of all Tooltips for Buildings/Units
--

local function generateTooltips(obj, name, resetfunc, nxt, ...)
	if nxt then generateTooltips(nxt, ...) end
	obj = obj("get")

	GameInfo = obj;
	VP = nil;

	disableprint();
	include("InfoTooltipInclude");
	enableprint();

	VP.___________________________ = true;

	memdiff(gc());

	local t1, m1, mwaste = 0, 0, 0;

	local countdown = CLEANUP;
	timediff()

	for building in obj.Buildings() do
		GetHelpTextForBuilding(building.ID);

		countdown = countdown - 1
		if countdown == 0 then
			t1 = t1 + timediff()
			m1, mwaste = m1 + memdiff(), mwaste + memdiff(gc());
			countdown = CLEANUP;
			timediff();
		end
	end

	--[[
	for unit in obj.Units() do
		
		GetHelpTextForUnit(unit.ID,true) ;
		
		countdown = countdown - 1
		if countdown == 0 then
			t1 = t1+timediff()
			m1,mwaste = m1+memdiff(), mwaste+memdiff(gc()) ;
			countdown = CLEANUP ;
			timediff();
		end
	end
	--]]

	t1 = t1 + timediff();
	m1, mwaste = m1 + memdiff(), mwaste + memdiff(gc());

	if not nxt then
		print();
		print();
		print("Generating tooltips for Buildings");
		print("Memory stats given here include about 38'600'000 bytes of memory allocated by InfoTooltipInclude");
		print(
			"Approx: ~34'000'000 bytes for tables used as arguments to gaminfo's filtered iterators and 4'600'00 bytes used presumably for string operations")
		print("Main thing to look at here is time...")
		print();
		print("objname****", "time", "mem", "gc'ed", "delta");
		print();
	end
	print(name, t1, m1, mwaste, m1 + mwaste);
end

local function runtests0(...)
	loadAllTablesAndIterateOnce(...);

	--validateData(...)

	CLEANUP = 50000;
	testIterators(...);

	--	VPUI_OPTIMIZE_GIC = false;	
	--	CLEANUP = 27
	--	generateTooltips(...);

	VPUI_OPTIMIZE_GIC = true;
	CLEANUP = 27
	generateTooltips(...);
end

local function runtests(...)
	local __state = StateName;
	StateName = nil;

	local tmp = createGameInfoObject()
	for name in pairs(__PROTO) do
		local _ = tmp[name];
	end

	collectgarbage("stop"); -- Doesn't seem to actually work

	print("Running GameInfo test protocol(v2) ...");


	runtests0(...);

	collectgarbage("restart");

	print();
	StateName = __state;
end




local function createGiProv(obj, loadfunc, resetfunc)
	return (function(op)
		if op == "get" then
			return obj;
		elseif op == "load" then
			if loadfunc then obj = loadfunc() end
			for tablename in pairs(__PROTO) do
				local _ = obj[tablename]
			end
			return obj;
		elseif op == "reset" and resetfunc and obj then
			resetfunc(obj);
		end
	end);
end





---- CPK


CPK = nil;
CPK_GI_ALWAYS_KEEP_ROWS = 1;
include("CPK.lua");

cpkGameInfo = createGiProv(nil, CPK.DB.GiAsm.Assemble);


---- newGameInfo (already old by now)

NEWGAMEINFO_NO_WARNINGS = 1 -- Would warn only about CustomModOptions string condition query when VPUI is reloaded ...
NEWGAMEINFO_USE_CLOSURE_ITERATORS = 1
createGameInfoObject = nil
include("newGameInfo.lua")
newGameInfo = createGiProv(nil, createGameInfoObject);


--[[
NEWGAMEINFO_USE_CLOSURE_ITERATORS = 0
NEWGAMEINFO_STATIC_ITERATOR_INVOKEONCE_COMPAT = 1
createGameInfoObject = nil
include("newGameInfo.lua")
zeroWasteCo = createGiProv( nil, createGameInfoObject ) ;
--]]


---- zipGameInfo (newGameInfo 2.0)


--[[
ZIPGAMEINFO_NO_WARNINGS = 1 ;
newZipGameInfo = nil ;
include("zipGameInfo");
if newZipGameInfo then
	zipGameInfo = createGiProv( nil, newZipGameInfo ) ;
end
--]]


------------------------------------

local emptyFunc = function() end;

local resetTable = function(obj)
	for k in pairs(__PROTO) do
		obj[k] = nil;
	end
end

local resetOldGameInfo = function(obj)
	resetTable(debug.getmetatable(obj).__index)
end

___GameInfo = createGiProv(oldGameInfo, nil, resetOldGameInfo)



local TESTOBJS = {
	___GameInfo, "oldGameInfo", 0,
	newGameInfo, "newGameInfo", 0,
	cpkGameInfo, "cpkGameInfo", 0,
	--	zeroWasteCo, "zeroWasteCo", 0 ;
	-- zipGameInfo, "zipGameInfo", 0 ;
}

local function reverseTable3(tab, index)
	local index = index or (#tab - 2);
	local a, b, c = tab[index], tab[index + 1], tab[index + 2];
	if index > 3 then
		reverseTable3(tab, index - 3);
	end
	index = #tab - index - 1;
	tab[index] = a;
	tab[index + 1] = b;
	tab[index + 2] = c;
end


reverseTable3(TESTOBJS)

runtests(unpack(TESTOBJS));
