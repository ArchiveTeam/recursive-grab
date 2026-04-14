local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local job_urls = cjson.decode(os.getenv("job_urls"))
local item_job = os.getenv("item_job")
local job_config = cjson.decode(os.getenv("job_config"))
local item_name = nil
local item_url = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered = {
  ["accepted"] = {},
  ["not_accepted"] = {},
  ["rejected"] = {}
}
local bad_items = {}

local retry_url = false

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  item = item_job .. ":" .. item
  if not discovered[target][item] then
    for _, key in pairs({"not_accepted", "rejected"}) do
      if target == "accepted"
        and discovered[key][item] then
        discovered[key][item] = nil
      elseif target == key
        and discovered["accepted"][item] then
        return false
      end
    end
print("discovered", target, item)
    discovered[target][item] = true
    return true
  end
  return false
end

report_bad_item = function(item)
  if item ~= nil then
    bad_items[item] = true
  end
end

normalize_url = function(url)
  local candidate_current = string.match(url, "^([^#]+)")
  while true do
    local temp = string.lower(urlparse.unescape(candidate_current))
    if temp == candidate_current then
      break
    end
    candidate_current = temp
  end
  if not string.match(candidate_current, "^https?://[^/]+/") then
    candidate_current = candidate_current .. "/"
  end
  for _, pattern in pairs({
    "^(https?://)[^/]*@([^/]+/.*)$",
    "^(https?://[^/:]+):[^/]+(.*)$"
  }) do
    local a, b = string.match(candidate_current, pattern)
    if a and b then
      candidate_current = a .. b
    end
  end
  candidate_current = string.gsub(candidate_current, "^https?", "")
  return candidate_current
end

local urls = {}
for _, url in pairs(job_urls) do
  urls[normalize_url(url)] = true
end

set_item = function(url)
  local candidate_current = normalize_url(url)
  if candidate_current ~= item_url and urls[candidate_current] then
    item_url = candidate_current
    item_name = item_job .. ":" .. item_url
  end
end

find_path_loop = function(url, max_repetitions)
  if not max_repetitions then
    return false
  end
  local tested = {}
  local tempurl = urlparse.unescape(url)
  tempurl = string.match(tempurl, "^https?://[^/]+(.*)$")
  if not tempurl then
    return false
  end
  for s in string.gmatch(tempurl, "([^/%?&]+)") do
    s = string.lower(s)
    if not tested[s] then
      if s == "" then
        tested[s] = -2
      elseif string.match(s, "^[0-9]+$") then
        tested[s] = -1
      else
        tested[s] = 0
      end
    end
    tested[s] = tested[s] + 1
    if tested[s] == max_repetitions then
      return true
    end
  end
  return false
end

maybe_accept = function(candidate, accept)
  for _, pattern in pairs(job_config["reject"] or {}) do
    if string.match(candidate, pattern) then
      discover_item("rejected", candidate)
      return "rejected"
    end
  end
  for _, pattern in pairs(job_config["~reject"] or {}) do
    if not string.match(candidate, pattern) then
      discover_item("rejected", candidate)
      return "rejected"
    end
  end
  if find_path_loop(candidate, job_config["max_path_loop_depth"]) then
    discover_item("rejected", candidate)
    return "rejected"
  end
  if accept then
    discover_item("accepted", candidate)
    return "accepted"
  end
end

discovery_check = function(url, parenturl)
  for _, pattern in pairs(job_config["accept"] or {}) do
    if string.match(url, pattern) then
      if maybe_accept(url, true) then
        return true
      end
    end
  end
  for _, pattern in pairs(job_config["~accept"] or {}) do
    if not string.match(url, pattern) then
      if maybe_accept(url, true) then
        return true
      end
    end
  end

  if parenturl then
    for parent_pattern, patterns in pairs(job_config["accept_with_parent"] or {}) do
      if string.match(parenturl, parent_pattern) then
        if type(patterns) == "string" then
          patterns = {patterns}
        end
        for _, pattern in pairs(patterns) do
          if string.match(url, pattern) then
            if maybe_accept(url, true) then
              return true
            end
          end
        end
      end
    end
  end

  discover_item("not_accepted", url)

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local parenturl = parent and parent["url"] or nil

  if job_config["accept_refresh"]
    and urlpos["link_refresh_p"] ~= 0 then
    maybe_accept(url, true)
  end

  if job_config["accept_inline"]
    and urlpos["link_inline_p"] ~= 0 then
    maybe_accept(url, true)
  end

  discovery_check(url, parenturl)

  return false
end

bad_code = function(status_code)
  for _, code in pairs(job_config["status_codes"]["accept"]) do
    if status_code == code then
      return false
    end
  end
  return true
end

wget.callbacks.write_to_warc = function(url, http_stat)
  local status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  if bad_code(http_stat["statcode"]) then
    print("Not writing to WARC.")
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  local status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = nil
    if http_stat["newloc"] then
      newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    end
    if newloc and (
      status_code == 301
      or status_code == 303
      or status_code == 308
    ) then
      discovery_check(newloc, url["url"])
      return wget.actions.EXIT
    end
  end

  if downloaded[url["url"]] then
    report_bad_item(item_name)
    return wget.actions.EXIT
  end

  if bad_code(status_code) or retry_url then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. ").\n")
    io.stdout:flush()
    report_bad_item(item_name)
    return wget.actions.EXIT
  end

  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  local targets = {}
  for name, key in pairs(job_config["keys"]) do
    targets["recursive-" .. item_job .. "-" .. name .. "-" .. key .. "?shard=job-" .. item_job .. "-" .. name] = discovered[name]
  end
  for key, data in pairs(targets) do
    print("queuing for", string.match(key, "^([^%?]+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

