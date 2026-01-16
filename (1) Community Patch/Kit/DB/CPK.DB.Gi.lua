local GiAsm = CPK.DB.GiAsm

local function gc()
	collectgarbage('collect')
	collectgarbage('collect')
	collectgarbage('collect')
end

gc()
GiAsm.Assemble()
gc()
gc()
gc()

local before_kb = collectgarbage('count')

local Gi = GiAsm.Assemble()
GiAsm.Assemble = nil
gc()
gc()
gc()
local after_kb = collectgarbage('count')

print('Gi consumed', (after_kb - before_kb) / 1024, 'MB')

CPK.DB.Gi = Gi
