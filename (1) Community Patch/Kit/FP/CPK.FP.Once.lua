local function Once(val)
	local done = false
	return function()
		if done then return nil end
		done = true
		return val
	end
end

CPK.FP.Once = Once
