#!/usr/bin/lua
local uuid = require("uuid")
uuid.set_rng(uuid.rng.urandom())

local json = require("cjson.safe")
json.encode_escape_forward_slash(false)

Ltn12 = require("ltn12")
Http = require("socket.http")
Socket = require("socket")
Sleep = Socket.sleep

local file = io.open("./db_pass.txt", "r")
if file == nil then
	print("Unable to open DB Password file, aborting")
	return 1
end

local db_pass = file:read("a")
file:close()

local driver = require "luasql.mysql"
local env = assert(driver.mysql())
Conn = assert(env:connect("electracion", "electracion", db_pass))

local function print_json(data)
	data = json.encode(data)
	data = string.gsub(data, "\\", "")
	data = string.gsub(data, "\"{", "{")
	data = string.gsub(data, "}\"", "}")
	print(data)
end

local function print_table(data)
	if data == nil then return end
	for k,v in pairs(data) do
		print(k..": "..v)
	end
end

local function insert_data(data)
	print("Inserting data")

	if data == nil then
		print_json(data)
	end

	for _, value in pairs(data) do
		assert(
				Conn:execute(
					"INSERT IGNORE INTO weather_summary_daily "..
						"(id, date, data_type, station, attributes, value) "..
					"VALUES "..
						"("..
							"'"..uuid.v4().."',"..
							"'"..value["date"].."',"..
							"'"..value["datatype"].."',"..
							"'"..value["station"].."',"..
							"'"..value["attributes"].."',"
							..value["value"]..
						");"
				)
			)
	end
end

local function request(endpoint, start_date, end_date, offset, count)
	local req_body = "pAdvjhHfjaZvKCwmrVQKWLiGIdOJwKke"
	local resp_body = {}

	local url = "https://www.ncei.noaa.gov/cdo-web/api/v2/"..endpoint..
			"?datasetid=GHCND"..
			"&units=metric"..
			(offset ~= nil and "&offset="..offset or "")..
			"&limit=1000"

	if start_date ~= nil then
		url = url.."&startdate="..start_date.."&enddate="..end_date
	end

	print(url)

	local _, resp_status_code = Http.request {
		url = url,
		method = "GET",
		headers = {
			["Accept"] = "*/*",
			["Accept-Encoding"] = "gzip, deflate, br, zstd",
			["Accept-Language"] = "en-US,en;q=0.5",
			["content-length"] = string.len(req_body),
			["token"] = "pAdvjhHfjaZvKCwmrVQKWLiGIdOJwKke"
		},
		source = Ltn12.source.string(req_body),
		sink = Ltn12.sink.table(resp_body)
	}

	local msg = ""

	if type(resp_body) == "table" and resp_body ~= nil then
		local resp_body_count = 0
		for _,_ in pairs(resp_body) do resp_body_count = resp_body_count + 1 end
		if resp_body_count > 1 then resp_body[1] = table.concat(resp_body) end

		resp_body = resp_body[1]
		resp_json, msg = json.decode(resp_body)

		if msg ~= "" and msg ~= nil then
			print(msg)
			print(resp_body)
			resp_status_code = -1
		else
			offset = resp_json["metadata"]["resultset"]["offset"]
			count = resp_json["metadata"]["resultset"]["count"]
		end
	elseif resp_status_code == 200 or resp_status_code == '200' then
		count = 0
	end

	return resp_json, resp_status_code, offset, count
end

local function request_all(endpoint, start_date, end_date)
	local data = {}
	local offset = 0
	local count = 100000
	local resp_body = {}
	local status = 200
	local offset_increment = 1000
	local wait_time = 5
	local num_data = 0

	while (offset + offset_increment <= count) do
		print("Requesting offset: "..offset.." for endpoint: "..endpoint)

		resp_body, status, offset, count = request(endpoint, start_date, end_date, offset, count)

		if status == 200 then
			print("Response recieved.")

			for k,_ in pairs(resp_body["results"]) do
				table.insert(data, resp_body["results"][k])
				num_data = num_data + 1
			end

			if num_data > 10000 then
				if endpoint == "data" then
					insert_data(data)
					data = {}
					num_data = 0
				end
			end

			if count - offset < offset_increment then offset_increment = (count - offset) end
			offset = offset + offset_increment

			wait_time = 5
		else
			print("Non 200 response receieved ("..status..")")

			if type(resp_body) == "table" then
				print("Response body "..table.concat(resp_body))
			elseif resp_body ~= nil then
				print("Response body "..resp_body)
			end

			wait_time = wait_time * 2
		end

		Sleep(wait_time)
	end

	return data
end

local function insert_locations(locs)
	print("Inserting locations")

	if locs == nil then
		print_json(locs)
	end

	for _, value in pairs(locs) do
		local query =
					"INSERT IGNORE INTO weather_summary_locations "..
						"(id, name, data_coverage, min_date, max_date) "..
					"VALUES "..
						"('"..value["id"].."', '"..string.gsub(value["name"],"'","").."', "..value["datacoverage"]..", '"..value["mindate"].."', '"..value["maxdate"].."');"

		local resp = Conn:execute(query)
		if not resp then print("A query error occured: "..query) os.exit() end
	end
end

--[[
local locs = request_all("locations", nil, nil)
insert_locations(locs)
--]]

local year = 1923
local month = '05'
local day = '24'

while year.."-"..month.."-"..day ~= os.date("%Y-%m-%d") do
	print("Getting data for "..year.."-"..month.."-"..day)

	local data = request_all("data", year.."-"..month.."-"..day, (year + 1).."-"..month.."-"..day)
	insert_data(data)

	year = year + 1
end

env:close()
