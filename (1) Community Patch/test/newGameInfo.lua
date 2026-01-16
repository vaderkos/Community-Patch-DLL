--[[

	This is a fully compatible replacement for the atrocious GameInfo object provided by fireaxis' civ5 game engine...
	Created for CP/VP but works with stock Civ5 as well...
	
	For the 5.03 alpha version of VP with EUI replacing the stock GameInfo object with this, reduces lua's memory by about 25Mb from 50Mb total after loading a safegame (LuaJIT)
	Needs about 930Kb of memory to store everything requested from the databse when loading said safegame...
	
	Main design concepts:
		* Use arrays to store data of columns that are filled with mostly non defaults, because they are more memory efficient and faster
		* For other columns only store for each row the non default values
		* 'Rows' given by any functions here don't hold data, they just act as a kind of pointer to it, and retrieve it when indexed

	Common names for variables in this code:
	
	idx - index of a row in the storage system setup by this code
	prop/property - name of column
	root - a lua table object that holds all data of a single database table
	row - table that acts as a pointer to the data of a row
	GID - the ID of the row in the DB or the 1-based id to access it from the gameinfo object
	
	** Update 18/12/25 **
	- Memory usage alomst halfed again! (down to 930Kb from ~1700Kb)
	- Use table.fill to create efficient arrays on LuaJit (the old function abusing the stack actually worked only for stock Lua)
	- A more efficient storage system: Individual entries go into the same table with the same formula for position, but negated...
	- Whether a column is stored as array or not is now decided based on its entries, during runtime
	- No more parsing defaults from PRAGMA
	- Idx is now 0-based
	- Support ID's when they are sequential but don't start at 0
	- Doing all that reduced the code length by 40 lines!
	- Most of all CONST_NIL is now a function which is awsome
	
	** Update 3/1/26 **
	- supporting storage of previously created rows to reduce iteration garbage
	- rewrote gameid(GID) and storageid(IDX) handling
	- fixed some minor bugs
	- added some global flags, that serve similar roles to define pragmas in c (sadly this increased the length of the file somewhat)
--]]


-- Checked at compile time (when file is run)
local NEWGAMEINFO_KEEP_ROWS = NEWGAMEINFO_KEEP_ROWS or 1;
local NEWGAMEINFO_CREATE_LIST_FOR_TABLECON = NEWGAMEINFO_CREATE_LIST_FOR_TABLECON or 0;
local NEWGAMEINFO_MEMOIZE_Q2 = NEWGAMEINFO_MEMOIZE_Q2 or 1;

-- Checked at runtime (low frequency)
local NEWGAMEINFO_ASSIGN_TYPEKEYS = NEWGAMEINFO_ASSIGN_TYPEKEYS or 0;
local NEWGAMEINFO_NO_WARNINGS = NEWGAMEINFO_NO_WARNINGS or 0;


-- 100% better than CONST_NIL = {}
local function CONST_NIL(arg)
	if arg == nil then
		return CONST_NIL;
	elseif arg == CONST_NIL then
		return nil;
	else
		return arg;
	end
end

local assert = assert;
local collectgarbage = collectgarbage;
local type = type;
local setmetatable = setmetatable;
local tonumber = tonumber;
local tostring = tostring;
local ipairs = ipairs;
local pairs = pairs;
local error = error;
local table_fill = table.fill
local gprint = print;
local print = function(...) return gprint("[GAMEINFO]", ...) end
local origDB = DB;

local function Q(what)
	local ret = {};
	for row in origDB.Query(what) do
		ret[#ret + 1] = row
	end
	return ret;
end

local Q2;
if NEWGAMEINFO_MEMOIZE_Q2 == 1 then
	local Q2_CACHE = {}; --setmetatable({},{__mode="vk"}) ;
	Q2 = function(what, colname)
		local ret = Q2_CACHE[what .. colname];
		if not ret then
			ret = {};
			for row in origDB.Query(what) do
				ret[#ret + 1] = row[colname]
			end
			Q2_CACHE[what .. colname] = ret;
		end
		return ret;
	end
else
	Q2 = function(what, colname)
		local ret = {};
		for row in origDB.Query(what) do
			ret[#ret + 1] = row[colname]
		end
		return ret;
	end
end


-- Main lookup function!
-- root is the table, idx the index in our array/indivdata table and prop the name of the column
local function getProperty(root, idx, prop)
	local proto = root.__PROTO;
	local colidx = proto[prop] or 0;
	if colidx > 0 then
		return root.__ARRAYDATA[colidx + root.__NUMARRAYCOLS * idx];
	elseif colidx < 0 then
		local val = root.__ARRAYDATA[colidx - root.__NUMINDIVCOLS * idx];

		if val == nil then
			val = proto[colidx];
		end

		if val ~= nil then
			return CONST_NIL(val);
		else
			print("Something went wrong when looking up property: Table, idx, prop", root.__TABLENAME, idx, prop)
		end
	elseif (NEWGAMEINFO_NO_WARNINGS == 0) and (not root.__NOTACOLWARN) then
		root.__NOTACOLWARN = prop;
		print("Not a column", prop, "in table", root.__TABLENAME);
		print("This warning is only shown once per table!");
	end
end

local function convert_IDX_to_GID(root, idx)
	return (root.__IDXplusOne_TO_GID and root.__IDXplusOne_TO_GID[idx + 1]) or (idx + root.__GID_AT_IDX_ZERO);
end

local function convert_GID_to_IDX(root, gid)
	return (root.__GID_TO_IDX and root.__GID_TO_IDX[gid]) or (gid - root.__GID_AT_IDX_ZERO);
end

-- The metatable for a row returned by <ROOTTABLE>[key] or iterators <ROOTTABLE>()
local MT_ROW_INSTANCE = {
	__index = function(rowinstance, prop) return getProperty(rowinstance.__ROOT, rowinstance.__IDX, prop) end,
}

local function getRowFromIdxOrNil(root, idx)
	if idx and idx >= 0 and idx < root.__COUNT then
		return setmetatable({
			__IDX = idx,
			__ROOT = root,
		}, MT_ROW_INSTANCE);
	end
end

local getRowFromGidOrNilAndSet;
if NEWGAMEINFO_KEEP_ROWS == 1 then
	getRowFromGidOrNilAndSet = function(root, gid)
		local ret = getRowFromIdxOrNil(root, convert_GID_to_IDX(root, gid));
		if ret then
			root[gid] = ret
		end
		return ret;
	end
else
	getRowFromGidOrNilAndSet = function(root, gid)
		return getRowFromIdxOrNil(root, convert_GID_to_IDX(root, gid));
	end
end

-- Some mods do 'for row in Game and GameInfo.Buildings() or DB.Query("SELECT * FROM Buildings") do {BLOCK} end' which is not exactly how lua's 'for' iterators are supposed to work. This means we need a closure...
-- See: https://lua.org/manual/5.1/manual.html#2.4.5
local function createIteratorClosureFilter(root, filter)
	local iteratoridx = 0;
	return (function()
		iteratoridx = iteratoridx + 1;
		return (filter[iteratoridx] and root[filter[iteratoridx]]) or nil;
	end);
end

local function createIteratorClosure(root)
	if root.__IDXplusOne_TO_GID then
		return createIteratorClosureFilter(root, root.__IDXplusOne_TO_GID)
	end
	local iteratoridx = root.__GID_AT_IDX_ZERO - 1;
	return (function()
		iteratoridx = iteratoridx + 1;
		return root[iteratoridx];
	end);
end

local createIteratorFromTableCon;
if NEWGAMEINFO_CREATE_LIST_FOR_TABLECON == 1 then
	createIteratorFromTableCon = function(root, filterarg)
		local ret = {}
		for idx = 0, root.__COUNT - 1 do
			local pass = true;
			for k, v in pairs(filterarg) do
				local prop = getProperty(root, idx, k);
				if prop ~= v and not (prop == true and v == 1) then -- {PrereqTech = techType, ShowInTechTree = 1} , used in stock civ5. Unhappy :/
					pass = false;
					break;
				end
			end
			if pass then ret[#ret + 1] = convert_IDX_to_GID(root, idx) end
		end
		return createIteratorClosureFilter(root, ret);
	end
else
	createIteratorFromTableCon = function(root, filterarg)
		local idx, count = -1, root.__COUNT, true;
		local ret = function()
			while (idx < count) do
				idx = idx + 1;
				local pass = true;
				for k, v in pairs(filterarg) do
					local prop = getProperty(root, idx, k);
					if prop ~= v and not (prop == true and v == 1) then -- {PrereqTech = techType, ShowInTechTree = 1} , used in stock civ5. Unhappy :/
						pass = false;
						break;
					end
				end
				if pass then return root[convert_IDX_to_GID(root, idx)] end
			end
			return nil;
		end
		return ret;
	end
end

-- EG: GI.Buildings(); GI.Buildings("Cost>500"); GI.Buildings({Cost = 200})
-- Returns an iterator, that returns a row each time it's called
local function callRootTable(root, filterarg)
	if filterarg == nil then
		return createIteratorClosure(root)
	end

	if type(filterarg) == "table" then
		return createIteratorFromTableCon(root, filterarg);
	end

	assert(type(filterarg) == "string", "Filter has to be a value of type table or string!");

	if root.__IDCOL then -- Not going to implement a parser for generic SQL string conditions, but if the table has an ID column we can request just those from the DB
		local querystring = "SELECT ID AS luaid FROM " .. root.__TABLENAME .. " WHERE " .. filterarg;
		return createIteratorClosureFilter(root, Q2(querystring, "luaid"));
	end

	-- Vaderkos <3 suggests rowids, but this case is so rare in CP/VP/EUI that I will stick with this fake solution, as storing rowids would take quite alot of space
	local querystring = "SELECT * FROM " .. root.__TABLENAME .. " WHERE " .. filterarg;
	if (NEWGAMEINFO_NO_WARNINGS == 0) and (root.__FILTERARGWARNING == false) then
		print("Can't handle filterarg for table:", root.__TABLENAME, "Arg:", filterarg,
			"Redirecting to original database with query:", querystring);
		print("This warning is only shown once per table!");
		root.__FILTERARGWARNING = filterarg;
	end
	return origDB.Query(querystring);
end

-- EG: GI.Buildings[0] or GI.Buildings.BUILDING_COURTHOUSE
local function indexRootTable(root, token)
	if type(token) == "number" then
		return getRowFromGidOrNilAndSet(root, token);
	elseif type(token) == "string" then
		local typecol = root.__PROTO["__type"]
		if typecol and typecol > 0 then
			local array, step = root.__ARRAYDATA, root.__NUMARRAYCOLS;
			for idx = 0, root.__COUNT - 1 do
				if array[typecol + idx * step] == token then
					local ret = root[convert_IDX_to_GID(root, idx)];
					if NEWGAMEINFO_ASSIGN_TYPEKEYS == 1 then -- checked at runtime, because it's rather rare
						root[token] = ret;
					end
					return ret;
				end
			end
			return nil
		else
			print("Indexing by type not supported for this table!", root.__TABLENAME, token);
			return nil;
		end
	else
		if token == nil then -- EUI compat, original GameInfo would (also) scream!
			return nil;
		else
			error(
			"Can only index by number(ID column if available, otherwise (undefined) cache ordering) or string(if the table has a type column)");
		end
	end
end

local MT_ROOT_INSTANCE = {
	__call = callRootTable,
	__index = indexRootTable,
}

local function setProtodata(root, tablename)
	local db_proto = Q("PRAGMA table_info(" .. tablename .. ")");
	if #db_proto < 1 then error("No such table! '" .. tostring(tablename) .. "'") end;

	local db_count = Q("SELECT COUNT() AS C FROM " .. tablename)[1]["C"];

	local proto = {};
	for _, row in ipairs(db_proto) do
		proto[row.name] = 0;
	end

	root.__PROTO = proto;
	root.__COUNT = db_count;

	root.__FILTERARGWARNING = false;
	root.__NOTACOLWARN = false;
end

local function copyDataFromDB(root, tablename)
	local proto = root.__PROTO;
	local db_data;
	local idcol = proto["ID"];
	local entries = root.__COUNT;

	if idcol then
		db_data = Q("SELECT * FROM " .. tablename .. " ORDER BY ID");
	else
		db_data = Q("SELECT * FROM " .. tablename);
	end

	local arraycols, indivcols = 0, 0;
	local defvals = {};

	for k in pairs(proto) do
		local vals = {};
		local currval, currcount = nil, 0;

		if (k == "Type" or k == "ID" or k == "Name") and false then
			--currcount = -10000 ;
		else
			for _, row in ipairs(db_data) do
				local val = CONST_NIL(row[k]);
				vals[val] = (vals[val] and (vals[val] + 1)) or 1;
			end
			for val, count in pairs(vals) do
				if count > currcount then
					currval, currcount = val, count
				end
			end
		end

		if (16 * entries - 24 * currcount + 64) > 0 then -- This is voodoo...
			arraycols = arraycols + 1;
			proto[k] = arraycols;
		else
			indivcols = indivcols + 1;
			proto[k] = -indivcols;
			defvals[-indivcols] = currval;
		end
	end

	local array = table_fill(CONST_NIL, entries * arraycols);

	for dataidx, datarow in ipairs(db_data) do
		local val;
		for key, colidx in pairs(proto) do
			val = datarow[key];

			if colidx > 0 then
				array[colidx + arraycols * (dataidx - 1)] = val;
			elseif colidx < 0 and CONST_NIL(val) ~= defvals[colidx] then
				array[colidx - indivcols * (dataidx - 1)] = CONST_NIL(val);
			end
		end
	end

	for colidx, defval in pairs(defvals) do
		proto[colidx] = defval;
	end

	root.__GID_TO_IDX = false;
	root.__IDXplusOne_TO_GID = false;

	root.__GID_AT_IDX_ZERO = false;

	if idcol and entries > 0 and type(array[proto["ID"]]) == "number" then
		idcol = proto["ID"]
		local idpos_0 = array[idcol];
		local non_sequential = false;
		for idx = 1, entries do
			if array[idcol + (idx - 1) * arraycols] ~= (idx - 1) + idpos_0 then
				print("ID column found, but IDs are not sequential!", tablename); --Only vanilla, CP/VP has a remapper
				print(idcol, arraycols, entries, idx)
				non_sequential = true;
				break;
			end
		end

		if non_sequential then
			local reverseTable1, reverseTable2 = table_fill(-1, entries), table_fill(-1, entries);
			for idx = 1, entries do
				reverseTable1[array[idcol + (idx - 1) * arraycols]] = idx - 1;
				reverseTable2[idx] = array[idcol + (idx - 1) * arraycols];
			end
			root.__GID_TO_IDX = reverseTable1;
			root.__IDXplusOne_TO_GID = reverseTable2;
		else
			root.__GID_AT_IDX_ZERO = idpos_0;
		end
	else
		root.__GID_AT_IDX_ZERO = 1;
	end

	if proto["Type"] then
		proto["__type"] = proto["Type"];
	elseif proto["Name"] then
		proto["__type"] = proto["Name"];
	end

	root.__IDCOL = idcol or false;
	root.__ARRAYDATA = array or false;
	root.__NUMINDIVCOLS = indivcols or false;
	root.__NUMARRAYCOLS = arraycols or false;
end

local function indexGameInfo(GI, tablename)
	if type(tablename) ~= "string" then error("Invalid tablename!") end

	local root = { __TABLENAME = tablename };

	setProtodata(root, tablename);
	copyDataFromDB(root, tablename);

	GI[tablename] = root;
	return setmetatable(root, MT_ROOT_INSTANCE);
end


local MT_GAMEINFO_OBJECT = {
	__call = function(GI, ops)
		if ops == "resetprint" then
			collectgarbage("collect");
			local m = collectgarbage("count") * 1024;
			local tot = 0;
			print()
			print("Releasing tables: bytes freed / DB table name")
			for k in pairs(GI) do
				GI[k] = nil;
				collectgarbage("collect");
				local m2 = collectgarbage("count") * 1024;
				print(m - m2, k);
				tot = tot + m - m2;
				m = m2;
			end
			print(tot, "Total");
			print();
		elseif ops == "reset" then
			for k in pairs(GI) do GI[k] = nil end
			print("All tables reset!")
		end
		return GI;
	end,
	__index = indexGameInfo,
}

function createGameInfoObject(GI)
	return setmetatable(GI or {}, MT_GAMEINFO_OBJECT);
end

newGameInfo = createGameInfoObject()
if not oldGameInfo then
	oldGameInfo = GameInfo;
end
