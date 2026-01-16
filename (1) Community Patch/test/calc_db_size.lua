local sql1 = [[
	SELECT name FROM `sqlite_master`
	WHERE type = 'table' AND name NOT LIKE 'sqlite%'
]]

local sql2 = 'SELECT * FROM `%s` ORDER BY _ROWID_'

local function gc_now()
	collectgarbage('collect')
	collectgarbage('collect')
	collectgarbage('collect')
end

local function mem_kb()
	return collectgarbage('count')
end

----------------------------------------------------------------
-- Phase 1: collect uniques (kept alive)
----------------------------------------------------------------

local KEY_SET = {}
local VAL_SET = {}

for tbl in DB.Query(sql1) do
	for row in DB.Query(string.format(sql2, tbl.name)) do
		for k, v in next, row do
			if v ~= nil then
				KEY_SET[k] = true
				VAL_SET[v] = true
			end
		end
	end
end

----------------------------------------------------------------
-- Phase 2: count uniques
----------------------------------------------------------------

local key_cnt = 0
for _ in next, KEY_SET do key_cnt = key_cnt + 1 end

local val_cnt = 0
for _ in next, VAL_SET do val_cnt = val_cnt + 1 end

----------------------------------------------------------------
-- Phase 3: baseline with sets alive
----------------------------------------------------------------

gc_now()
local before = mem_kb()

----------------------------------------------------------------
-- Phase 4: allocate minimal arrays
----------------------------------------------------------------

local KEYS = table.fill(nil, key_cnt)
local VALS = table.fill(nil, val_cnt)

do
	local i = 1
	for k in next, KEY_SET do
		KEYS[i] = k
		i = i + 1
	end
end

do
	local i = 1
	for v in next, VAL_SET do
		VALS[i] = v
		i = i + 1
	end
end

gc_now()
local after = mem_kb()

print(string.format(
	"Incremental memory for minimal representation: %.6f MB",
	(after - before) / 1024
))
