(function()
	if InstanceManager and GenerationalInstanceManager then
		return
	end

	local lua_setmetatable = setmetatable
	local lua_table_remove = table.remove
	local lua_table_insert = table.insert
	

	--- @class InstanceManager
	local InstanceManagerImpl = {}



	InstanceManager = {}
	InstanceManager.__index = InstanceManagerImpl

	--- Creates new InstanceManager
	--- @param _ InstanceManager # Ignored
	--- @param inst_name string
	--- @param root_name string
	--- @param parent Control
	--- @return InstanceManager
	function InstanceManager.new(_, inst_name, root_name, parent)
		return lua_setmetatable({
			inst_name = inst_name,
			root_name = root_name,
			parent = parent,
		}, InstanceManager)
	end

--------------------------------------------------------------------------------

end)()


GenerationalInstanceManager = GenerationalInstanceManager or (function()
	
	
	--- @class GenerationalInstanceManager
	local GimImpl = {}

	function GimImpl:GetInstance()

	end

	function GimImpl:Buid

	local GenerationalInstanceManager = {}
	GenerationalInstanceManager.__index = GimImpl

	--- @param _ any # Unused
	--- @param inst_name string # Instance name
	--- @param root_name string # Root control name
	--- @param parent Control # Parent control
	--- @return GenerationalInstanceManager
	function GenerationalInstanceManager.new(_, inst_name, root_name, parent)
		return lua_setmetatable({})
	end

	return GenerationalInstanceManager
end)()


InstanceManager = {
	------------------------------------------------------------------
	-- default values
	------------------------------------------------------------------
	--m_InstanceName;
	--m_RootControlName;
	m_iCount = 0,
	m_iAllocatedInstances = 0,
	m_iAvailableInstances = 0,
	--m_ParentControl;

	------------------------------------------------------------------
	-- constructor
	------------------------------------------------------------------
	new = function(self, instanceName, rootControlName, ParentControl)
		local o = {};
		setmetatable(o, self);
		self.__index           = self;

		o.m_InstanceName       = instanceName;
		o.m_RootControlName    = rootControlName;
		o.m_ParentControl      = ParentControl;
		o.m_AvailableInstances = {};
		o.m_AllocatedInstances = {};

		return o;
	end,


	------------------------------------------------------------------
	------------------------------------------------------------------
	GetInstance = function(self)
		if (#self.m_AvailableInstances == 0)
		then
			self:BuildInstance();
		end

		local instance = table.remove(self.m_AvailableInstances);
		instance[self.m_RootControlName]:SetHide(false);
		table.insert(self.m_AllocatedInstances, instance);

		self.m_iAvailableInstances = self.m_iAvailableInstances - 1;
		self.m_iAllocatedInstances = self.m_iAllocatedInstances + 1;

		return instance;
	end,


	------------------------------------------------------------------
	-- return an instance to the pool
	------------------------------------------------------------------
	ReleaseInstance = function(self, instance)
		if (instance == nil) then
			print("Instance Error: Release requested on nil instance [" ..
				self.m_InstanceName .. "] [" .. self.m_RootControlName .. "]");
			return;
		elseif (instance.m_InstanceManager ~= self) then
			print("Instance Error: Release requested on illegal instance [" ..
				self.m_InstanceName .. "] [" .. self.m_RootControlName .. "]");
		end

		for i, iter in ipairs(self.m_AllocatedInstances)
		do
			if (iter == instance)
			then
				iter[self.m_RootControlName]:SetHide(true);
				table.remove(self.m_AllocatedInstances, i);

				table.insert(self.m_AvailableInstances, instance);

				self.m_iAvailableInstances = self.m_iAvailableInstances + 1;
				self.m_iAllocatedInstances = self.m_iAllocatedInstances - 1;
			end
		end
	end,


	-------------------------------------------------
	-- build new instances
	-------------------------------------------------
	BuildInstance = function(self)
		local controlTable = {}

		if (self.m_ParentControl == nil)
		then
			ContextPtr:BuildInstance(self.m_InstanceName, controlTable);
		else
			ContextPtr:BuildInstanceForControl(self.m_InstanceName, controlTable, self.m_ParentControl);
		end

		if (controlTable[self.m_RootControlName] == nil)
		then
			print("Instance Manager built with bad Root Control [" ..
				self.m_InstanceName .. "] [" .. self.m_RootControlName .. "]");
		end

		controlTable[self.m_RootControlName]:SetHide(true);
		controlTable.m_InstanceManager = self;
		table.insert(self.m_AvailableInstances, controlTable);
		self.m_iAvailableInstances = self.m_iAvailableInstances + 1;

		self.m_iCount = self.m_iCount + 1;
	end,


	-------------------------------------------------
	-- move all the instances back to the available
	-- list and hide the specified control
	-------------------------------------------------
	ResetInstances = function(self)
		for i = 1, #self.m_AllocatedInstances, 1
		do
			local iter = table.remove(self.m_AllocatedInstances);
			iter[self.m_RootControlName]:SetHide(true);
			table.insert(self.m_AvailableInstances, iter);
		end

		self.m_iAvailableInstances = self.m_iCount;
		self.m_iAllocatedInstances = 0;
	end,

	-------------------------------------------------
	-- Destroy all instances
	-------------------------------------------------
	DestroyInstances = function(self)
		self:ResetInstances();

		for i = 1, #self.m_AvailableInstances, 1
		do
			local iter = table.remove(self.m_AvailableInstances);
			if (self.m_ParentControl == nil)
			then
				ContextPtr:DestroyChild(iter);
			else
				self.m_ParentControl:DestroyChild(iter[self.m_RootControlName]);
			end
		end

		self.m_iAvailableInstances = 0;
	end,
}

-- This is similar to Instance Manager with one critical difference.
-- GetInstance will only return control instances that are younger than previously returned instances.
-- This is particularly useful if your instances are in a stack and you are refreshing under the assumption
-- that the controls will be populated one after the other.
-- This is impossible with the original InstanceManager and would require a call to SortChildren.
-- SortChildren, however, screws up any layout-dependent styles such as piano keys.
GenerationalInstanceManager =
{
	------------------------------------------------------------------
	-- constructor
	------------------------------------------------------------------
	new = function(self, instanceName, rootControlName, ParentControl)
		local o = {};
		setmetatable(o, self);
		self.__index          = self;

		o.m_InstanceName      = instanceName;
		o.m_RootControlName   = rootControlName;
		o.m_ParentControl     = ParentControl;
		o.m_Instances         = {};
		o.m_NextInstanceIndex = 1;
		return o;
	end,


	------------------------------------------------------------------
	------------------------------------------------------------------
	GetInstance = function(self)
		local nextInstanceIndex = self.m_NextInstanceIndex;

		if (nextInstanceIndex > #self.m_Instances) then
			self:BuildInstance();
		end

		local instance = self.m_Instances[nextInstanceIndex];
		instance[self.m_RootControlName]:SetHide(false);

		self.m_NextInstanceIndex = nextInstanceIndex + 1;

		return instance;
	end,

	-------------------------------------------------
	-- build new instances
	-------------------------------------------------
	BuildInstance = function(self)
		local controlTable = {}

		if (self.m_ParentControl == nil) then
			ContextPtr:BuildInstance(self.m_InstanceName, controlTable);
		else
			ContextPtr:BuildInstanceForControl(self.m_InstanceName, controlTable, self.m_ParentControl);
		end

		if (controlTable[self.m_RootControlName] == nil) then
			print("Instance Manager built with bad Root Control [" ..
				self.m_InstanceName .. "] [" .. self.m_RootControlName .. "]");
		end

		controlTable[self.m_RootControlName]:SetHide(true);
		controlTable.m_InstanceManager = self;
		table.insert(self.m_Instances, controlTable);
	end,


	-------------------------------------------------
	-- move all the instances back to the available
	-- list and hide the specified control
	-------------------------------------------------
	ResetInstances = function(self)
		--Hide all instances and reset counter.
		for i, v in ipairs(self.m_Instances) do
			v[self.m_RootControlName]:SetHide(true);
		end

		self.m_NextInstanceIndex = 1;
	end,

}
