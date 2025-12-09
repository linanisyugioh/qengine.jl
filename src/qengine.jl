module qengine
using HDB
using Dates
using CBinding
using StringEncodings
using Statistics
using Distributed
using dolphindb
using FinancialStruct
using FinancialStruct:FuturesTick,SecurityTick,HDataItem,HCodeInfo,CodeInfo
import FinancialStruct.FuturesTick as kline

#holidayfile = "X:/hdb_data/download/holidayinfo.txt"
#holidayfile = "Z:/hdb_data/download/holidayinfo.txt"
#orderfee_file = "D:/workspace/期货手续费文件/order_fee.csv"
#begin_date = Date(2024,9,1)
#end_date = Date(2025,3,24)
#folder = "Z:/hdb_data/"
##folder = "X:/hdb_data/"
#logdir = "D:/workspace/strategy01/log/"
#save_mode = 4
#savepath = "D:/workspace/strategy01/result/"
#save_mode: 1: save to csv, 2：save to dolphindb, 
#           3: push to orderlistch, than save to csv
#           4: push to orderlistch, than save to dolphindb
#host = "100.125.11.222"
#port = 8848
#usr = "linan"
#psw = "linan@2023"
#table_name = "lftorder_stream"
#db_path = "dfs://backtest"
#dolphindb.init(host, port, usr, psw, table_name, db_path)
#dolphindb.init(host, port, usr, psw, table_name)
#dolphindb.init()

#include(string(hdbpath,"struct/marketdata.jl"))
#dates = trade_date(holidayfile, begin_date, end_date)
flags = 0
orderlistch = Channel{Dict{String,Vector}}(Inf)
fee_security = 0.00075
trade_time = Dict{String, Integer}(
"SHFE.rb"=>230000000,
"SHFE.hc"=>230000000,
"SHFE.fu"=>230000000,
"SHFE.bu"=>230000000,
"SHFE.ru"=>230000000,
"SHFE.br"=>230000000,
"SHFE.sp"=>230000000,
"INE.nr"=>230000000,
"INE.lu"=>230000000,
"DCE.a"=>230000000,
"DCE.b"=>230000000,
"DCE.c"=>230000000,
"DCE.cs"=>230000000,
"DCE.m"=>230000000,
"DCE.p"=>230000000,
"DCE.rr"=>230000000,
"DCE.y"=>230000000,
"DCE.eb"=>230000000,
"DCE.eg"=>230000000,
"DCE.i"=>230000000,
"DCE.j"=>230000000,
"DCE.jm"=>230000000,
"DCE.l"=>230000000,
"DCE.pg"=>230000000,
"DCE.pp"=>230000000,
"DCE.v"=>230000000,
"CZCE.PF"=>230000000,
"CZCE.PR"=>230000000,
"CZCE.ZC"=>230000000,
"CZCE.MA"=>230000000,
"CZCE.TA"=>230000000,
"CZCE.SH"=>230000000,
"CZCE.PX"=>230000000,
"CZCE.SA"=>230000000,
"CZCE.FG"=>230000000,
"CZCE.SR"=>230000000,
"CZCE.CF"=>230000000,
"CZCE.CY"=>230000000,
"CZCE.RM"=>230000000,
"CZCE.OI"=>230000000,
"SHFE.cu"=>10000000,
"SHFE.al"=>10000000,
"SHFE.ao"=>10000000,
"SHFE.zn"=>10000000,
"SHFE.pb"=>10000000,
"SHFE.ni"=>10000000,
"SHFE.sn"=>10000000,
"SHFE.ss"=>10000000,
"INE.bc" =>10000000,
"SHFE.au"=>23000000,
"SHFE.ag"=>23000000,
"INE.sc"=>23000000)

#struct kline
#    time::Int32
#    status::Int32
#    pre_open_interest::Int64
#    pre_close::Int64
#    pre_settle_price::Int64
#    open::Int64
#    high::Int64
#    low::Int64
#    close::Int64
#    volume::Int64
#    turnover::Int64
#    open_interest::Int64
#    settle_price::Int64
#    high_limited::Int64
#    low_limited::Int64
#    pre_delta::Int32
#    curr_delta::Int32
#    ask_price::NTuple{5,Int64}
#    ask_vol::NTuple{5,UInt32}
#    bid_price::NTuple{5,Int64}
#    bid_vol::NTuple{5,UInt32}
#    trading_status::UInt8
#end
#export kline

#kline = FuturesTick

function FinancialStruct.FuturesTick(ftick::FuturesTick, kopen::Int64, khigh::Int64, klow::Int64, kclose::Int64,
               volume::Int64, turnover::Int64, pre_close::Int64)
#    ask_price = tuple(tickdata.ask_price...)
#    ask_vol = tuple(tickdata.ask_vol...)
#    bid_price = tuple(tickdata.bid_price...)
#    bid_vol = tuple(tickdata.bid_vol...)
#    kline(tickdata.time, tickdata.status, tickdata.pre_open_interest, pre_close,
#        tickdata.pre_settle_price, kopen, khigh, klow, kclose, volume, turnover, tickdata.open_interest,
#        tickdata.settle_price, tickdata.high_limited, tickdata.low_limited, tickdata.pre_delta,
#        tickdata.curr_delta, ask_price, ask_vol, bid_price, bid_vol, tickdata.trading_status)
    return FuturesTick(
        time = ftick.time,
        status = ftick.status,
        pre_open_interest = ftick.pre_open_interest,
        pre_close = pre_close,
        pre_settle_price = ftick.pre_settle_price,
        open = kopen,
        high = khigh,
        low = klow,
        match = kclose,
        volume = volume,
        turnover = turnover,
        open_interest = ftick.open_interest,
        close = kclose,
        settle_price = ftick.settle_price,
        high_limited = ftick.high_limited,
        low_limited = ftick.low_limited,
        pre_delta = ftick.pre_delta,
        curr_delta = ftick.curr_delta,
        ask_price = ftick.ask_price,# 确保类型匹配（如 Carray{Int64,5}）
        ask_vol = ftick.ask_vol,
        bid_price = ftick.bid_price,
        bid_vol = ftick.bid_vol,
        trading_status = UInt8(ftick.trading_status)  # 显式类型转换
    )        
end
export kline

function FinancialStruct.FuturesTick(tickdata::SecurityTick, kopen::Int64, khigh::Int64, klow::Int64, kclose::Int64,
               volume::Int64, turnover::Int64, pre_close::Int64)
#    ask_price = tuple(tickdata.ask_price[1:5]...)
#    ask_vol = tuple(tickdata.ask_vol[1:5]...)
#    bid_price = tuple(tickdata.bid_price[1:5]...)
#    bid_vol = tuple(tickdata.bid_vol[1:5]...)
#    kline(tickdata.time, tickdata.status, tickdata.pre_close, pre_close,
#        tickdata.pre_close, kopen, khigh, klow, kclose, volume, turnover, tickdata.open,
#        tickdata.match, tickdata.high_limited, tickdata.low_limited, Int32(1),
#        Int32(1), ask_price, ask_vol, bid_price, bid_vol, tickdata.trading_phase_code[1])
    return FuturesTick(
        time = tickdata.time,
        status = tickdata.status,
        pre_open_interest = Int64(tickdata.pre_close),
        pre_close = pre_close,
        pre_settle_price = Int64(tickdata.pre_close),
        open = kopen,
        high = khigh,
        low = klow,
        match = kclose,
        volume = volume,
        turnover = turnover,
        open_interest = Int64(tickdata.open),
        close = Int64(tickdata.match),
        settle_price = Int64(tickdata.open),
        high_limited = Int64(tickdata.high_limited),
        low_limited = Int64(tickdata.low_limited),
        pre_delta = Int32(1),
        curr_delta = Int32(1),
        ask_price = Carray{Int64, 5}(tickdata.ask_price...),# 确保类型匹配（如 Carray{Int64,5}）
        ask_vol = Carray{UInt32, 5}(tickdata.ask_vol...),
        bid_price = Carray{Int64, 5}(tickdata.bid_price...),
        bid_vol = Carray{UInt32, 5}(tickdata.bid_vol...),
        trading_status = UInt8(tickdata.trading_status)  # 显式类型转换
    )         
end
export kline

struct lftmatch
      high::Int64
      low::Int64
      ask_price::Int64
      ask_vol::UInt32
      bid_price::Int64
      bid_vol::UInt32
end
export lftmatch

mutable struct dataparam
    last_tradeday::Dict{String,Int}
    last_date::Dict{String,Int}
    last_timed::Dict{String,Int}
    last_volume::Dict{String,Int}
    last_turnover::Dict{String,Int}
	pre_close::Dict{String,Int}
    oneminute_prices::Dict{String,Vector{Int64}}
end

function dataparam()
    last_tradeday = Dict{String,Int}()
    last_date = Dict{String,Int}()
    last_timed = Dict{String,Int}()
    last_volume = Dict{String,Int}()
    last_turnover = Dict{String,Int}()
	pre_close = Dict{String,Int}()
    oneminute_prices = Dict{String,Vector{Int64}}()
    dataparam(last_tradeday, last_date, last_timed, last_volume, last_turnover, pre_close, oneminute_prices)
end
export dataparam

futureticks2d = Vector{Vector{FuturesTick}}()
securityticks2d = Vector{Vector{SecurityTick}}()
#custom2d
margin_ratio2d = Vector{Dict{String,NTuple{2,Integer}}}()#第一个是多头保证金率；第二个是空头保证金率
price_tick2d = Vector{Dict{String, Integer}}()
multiplier2d = Vector{Dict{String,Integer}}()
settleprice2d = Vector{Dict{String,Integer}}()
symbols2d = Vector{Vector{String}}()
datetimes2d = Vector{Vector{NTuple{2,Integer}}}()
lftmatch2d = Vector{Vector{lftmatch}}()
major_codes2d = Vector{Vector{String}}()
today_major_codes2d = Vector{Vector{String}}() #当前交易日的主力合约
next_major_codes2d = Vector{Vector{String}}() #下个交易日即将变更的主力合约代码，有一定的未来数据的性质，使用需谨慎。
#custom_type::DataType
#custom_param::DataType
#
# export FuturesTick
# export symbols2d
# export datetimes2d
# export minkline2d
# export margin_ratio2d
# export price_tick2d
# export multiplier2d
# export dates
#
function setdates(begindate::Integer, enddate::Integer)
    global begin_date, end_date, dates
    begin_date = Date(string(begindate),"yyyymmdd")
    end_date = Date(string(enddate),"yyyymmdd")
    dates = trade_date(holidayfile, begin_date, end_date)
end

function setdates(begindate::Date, enddate::Date)
    global begin_date, end_date, dates
    begin_date = begindate
    end_date = enddate
    dates = trade_date(holidayfile, begin_date, end_date)
end

function setdates()
    global begin_date, end_date, dates
    dates = trade_date(holidayfile, begin_date, end_date)
end
export setdates

function getdates()
    global dates
    return dates
end
export getdates

function data_reset()
    global futureticks2d,margin_ratio2d,price_tick2d,multiplier2d,symbols2d,datetimes2d,dates,lftmatch2d,major_codes2d
    global securityticks2d,custom2d,settleprice2d
    empty!(securityticks2d)
    empty!(futureticks2d)
#    empty!(minkline2d)
    empty!(margin_ratio2d)
    empty!(price_tick2d)
    empty!(multiplier2d)    
    empty!(symbols2d)
    empty!(datetimes2d)
    empty!(dates)    
    empty!(lftmatch2d)
    empty!(major_codes2d)
    empty!(settleprice2d)
#    empty!(custom2d)
end
export data_reset

function getproduct(symbol::String)::String
    exchange = split(symbol,".")[1]
    if exchange in ["SZ","SH"]
        return symbol
    elseif exchange == "CZCE"
        return symbol[1:end-3]
    else
        return symbol[1:end-4]
    end
end
export getproduct

function CTime(Time0::Integer, Time1::Integer)
    dt = (div(Time0,10000000)-div(Time1,10000000))*3600+(div(Time0,100000)%100-div(Time1,100000)%100)*60
    dt = dt+(Time0%100000-Time1%100000)/1000
    return dt
end
export CTime

function MTime(Time0::Integer, Time1::Integer)
    am1 = 101500000
    am2 = 103000000
    openU = 113000000
    openD = 133000000
    nightU = 150000000
    nightD = 210000000
    dt = CTime(Time0, Time1)
    if (Time1<=am1) && (Time0>=am2)
        dt = dt - 15*60
    end
    if (Time1<=openU) && (Time0>=openD)
        dt = dt - 120*60
    end
    if (Time1<=nightU) && (Time0>=nightD)
        dt = dt - 360*60
    end
    return dt
end
export MTime

function MTime(Time0::Integer, Time1::Integer, isnt_trade_time::Vector{NTuple{2, Integer}})
    dt = CTime(Time0, Time1)
    for gap in isnt_trade_time
        if (Time1<=gap[1]) && (Time0>=gap[2])
            dt = dt - CTime(gap[2], gap[1])
        end
    end
    return dt
end
export MTime

function CFFEXMTime(Time0::Integer, Time1::Integer)
    openU = 113000000
    openD = 130000000
    dt = CTime(Time0, Time1)
    if (Time1<=openU) && (Time0>=openD)
        dt = dt - 120*60
    end
    return dt
end
export CFFEXMTime

function trade_time_gap(product::AbstractString)
    global trade_time
    exchange = split(product, ".")[1]
    isnt_trade_time = Vector{NTuple{2, Integer}}()
    if exchange in ["CFFEX","SH","SZ"]
        push!(isnt_trade_time, (0,93000000))
        push!(isnt_trade_time, (113000000,130000000))
        if product in ["CFFEX.TS","CFFEX.TF","CFFEX.T","CFFEX.TL"]
            push!(isnt_trade_time, (151500000,240000000))
        else
            push!(isnt_trade_time, (150000000,240000000))
        end
    else
        if product in keys(trade_time)
            night_time = trade_time[product]    
            if night_time < 90000000
                push!(isnt_trade_time, (night_time,90000000))
                push!(isnt_trade_time, (101500000,103000000))
                push!(isnt_trade_time, (113000000,133000000))
                push!(isnt_trade_time, (150000000,210000000))
            else
                push!(isnt_trade_time, (0,90000000))
                push!(isnt_trade_time, (101500000,103000000))
                push!(isnt_trade_time, (113000000,133000000))
                push!(isnt_trade_time, (150000000,210000000))
                push!(isnt_trade_time, (night_time,240000000))
            end
        else
            push!(isnt_trade_time, (0,90000000))
            push!(isnt_trade_time, (101500000,103000000))
            push!(isnt_trade_time, (113000000,133000000))
            push!(isnt_trade_time, (150000000,240000000))                      
        end
    end
    return isnt_trade_time
end
export trade_time_gap

function MDateTime(product::AbstractString, date1::Integer, time1::Integer, date2::Integer, time2::Integer)
    isnt_trade_time = trade_time_gap(product)
    if date2 < date1
        dt = MTime(time1, 0, isnt_trade_time) + MTime(240000000, time2, isnt_trade_time)       
    elseif date2 == date1     
        dt = MTime(time1, time2, isnt_trade_time)    
    else
        dt = -1
    end
	return dt
end
export MDateTime

function get_tradedate(dateint::Integer, timeint::Integer)::Int
    if timeint < 210000000
        tradeday = next_trade_date(Date(string(dateint),"yyyymmdd"))
        return parse(Int, Dates.format(tradeday,"yyyymmdd"))
    else
        return Int(dateint)
    end
end
export get_tradedate

function load_securityticks!(file_id::UInt64, items::Vector{HDataItem},
                             fticks::Vector{SecurityTick}, symbols::Vector{String})
    symbol_list=join(symbols,",")
    type_list = "SecurityTick"
    task_id = hdb_open_read_task(file_id, symbol_list, type_list)
    len = 2000
    while len == 2000
        itemsi = hdb_read_items(task_id, 2000)
        push!(items,itemsi...)
        fticksi = parse_data.(itemsi,SecurityTick)
        push!(fticks,fticksi...)
        len = length(itemsi)
    end
    hdb_close_read_task(task_id)
end

function load_futureticks!(file_id::UInt64, items::Vector{HDataItem},
                            fticks::Vector{FuturesTick})
    symbol_list="ZLHY.*"
    type_list = "FuturesTick"
    task_id = hdb_open_read_task(file_id, symbol_list, type_list)
    len = 2000
    while len == 2000
        itemsi = hdb_read_items(task_id, 2000)
        push!(items,itemsi...)
        fticksi = parse_data.(itemsi,FuturesTick)
        push!(fticks,fticksi...)
        len = length(itemsi)
    end
    hdb_close_read_task(task_id)
end

function load_futureticks!(file_id::UInt64, items::Vector{HDataItem},
                           fticks::Vector{FuturesTick}, symbols::Vector{String})
    symbol_list=join(symbols,",")
    type_list = "FuturesTick"
    task_id = hdb_open_read_task(file_id, symbol_list, type_list)
    len = 2000
    while len == 2000
        itemsi = hdb_read_items(task_id, 2000)
        push!(items,itemsi...)
        fticksi = parse_data.(itemsi,FuturesTick)
        push!(fticks,fticksi...)
        len = length(itemsi)
    end
    hdb_close_read_task(task_id)
end

function load_futureticks!(file_id::UInt64, items::Vector{HDataItem},
                            fticks::Vector{FuturesTick}, date::Integer)
    symbol_list="ZLHY.*"
    type_list = "FuturesTick"
    task_id = hdb_open_read_task(file_id, symbol_list, type_list, begin_date=date, end_date=date, begin_time = 142700000, end_time =153000000)
    len = 2000
    while len == 2000
        itemsi = hdb_read_items(task_id, 2000)
        push!(items,itemsi...)
        fticksi = parse_data.(itemsi,FuturesTick)
        push!(fticks,fticksi...)
        len = length(itemsi)
    end
    hdb_close_read_task(task_id)
end

function load_futureticks!(file_id::UInt64, items::Vector{HDataItem},
                           fticks::Vector{FuturesTick}, symbols::Vector{String}, date::Integer)
    symbol_list=join(symbols,",")
    type_list = "FuturesTick"
    task_id = hdb_open_read_task(file_id, symbol_list, type_list, begin_date=date, end_date=date, begin_time = 142700000, end_time =153000000)
    len = 2000
    while len == 2000
        itemsi = hdb_read_items(task_id, 2000)
        push!(items,itemsi...)
        fticksi = parse_data.(itemsi,FuturesTick)
        push!(fticks,fticksi...)
        len = length(itemsi)
    end
    hdb_close_read_task(task_id)
end

function load_major_codeinfo!(file_id::UInt64, margin_ratio::Dict{String,NTuple{2,Integer}},
        price_tick::Dict{String, Integer}, multiplier::Dict{String,Integer}, major_codes::Vector{String})
    items = hdb_read_codetable(file_id, "ZLHY")
    codeinfodata = parse_data.(items, CodeInfo)
    symbols = parse_symbols(items)
    empty!(major_codes)
    for i in eachindex(items)
        symbol = symbols[i]
        push!(major_codes, symbol)
        margin_ratio[symbol] = (codeinfodata[i].margin_ratio_param1, codeinfodata[i].margin_ratio_param2)
        price_tick[symbol] = codeinfodata[i].price_tick
        multiplier[symbol] = codeinfodata[i].multiplier
    end
end

function load_major_codeinfo!(file_id::UInt64, margin_ratio::Dict{String,NTuple{2,Integer}},
    price_tick::Dict{String, Integer}, multiplier::Dict{String,Integer}, major_codes::Vector{String},
    products::Vector{String})
    items = hdb_read_codetable(file_id, "ZLHY")
    codeinfodata = parse_data.(items, CodeInfo)
    symbols = parse_symbols(items)
    empty!(major_codes)
    for i in eachindex(items)
        symbol = symbols[i]
        if getproduct(symbol) in products
            margin_ratio[symbol] = (codeinfodata[i].margin_ratio_param1, codeinfodata[i].margin_ratio_param2)
            price_tick[symbol] = codeinfodata[i].price_tick
            multiplier[symbol] = codeinfodata[i].multiplier 
            push!(major_codes, symbol)
        end
    end
end

function load_codeinfo!(file_id::UInt64, margin_ratio::Dict{String,NTuple{2,Integer}},
    price_tick::Dict{String, Integer}, multiplier::Dict{String,Integer},
    symbols::Vector{String})
    for symbol in symbols
        res = hdb_read_codeinfo(file_id, symbol)
        if isnothing(res)
        else
            codeinfodata = parse_data(res, CodeInfo)
            margin_ratio[symbol] = (codeinfodata.margin_ratio_param1, codeinfodata.margin_ratio_param2)
            price_tick[symbol] = codeinfodata.price_tick
            multiplier[symbol] = codeinfodata.multiplier 
        end
    end
end

#function parse_datetime(item::HDataItem)
#    datet = parse(Int32,Libc.strftime("%Y%m%d",item.local_time/1000))
#    timet = parse(Int32,Libc.strftime("%H%M%S",item.local_time/1000))*1000 + rem(item.local_time,1000)
#    return datet, timet
#end
#
#function parse_symbols(items::Vector{HDataItem})
#    ptr = pointer(items)
#    len = length(items)
#    ptrvec = convert.(Ptr{UInt8},[ptr+(i-1)*sizeof(HDataItem) for i = 1:len])
#    symbols = unsafe_string.(ptrvec)
#end
#
#function parse_symbols(items::Vector{HCodeInfo})
#    ptr = pointer(items)
#    len = length(items)
#    ptrvec = convert.(Ptr{UInt8},[ptr+(i-1)*sizeof(HCodeInfo) for i = 1:len])
#    symbols = unsafe_string.(ptrvec)
#end

function load_lftdata(dates::Vector{Date}, products_vec::Vector{Vector{String}},
        codes_vec::Vector{Vector{String}}, generate::Function,
        lftdata::DataType, param_type::DataType, io::IO, mode::Int)
    datesint = parse.(Int, Dates.format.(dates,"yyyymmdd"))
    global lftmatch2d, symbols2d, datetimes2d, margin_ratio2d, price_tick2d, multiplier2d
    global settleprice2d, major_codes2d, folder    
    db_id = hdb_open_db(folder)
    lftdata2d = Vector{Vector{lftdata}}()
    for i in eachindex(dates)
        lftdatavec = Vector{lftdata}()
        margin_ratio = Dict{String,NTuple{2,Integer}}()
        symbols = Vector{String}()
        datetimes = Vector{NTuple{2,Integer}}()
        price_tick = Dict{String, Integer}()
        multiplier = Dict{String,Integer}()
        settleprice = Dict{String,Integer}()
        lftmatchvec = Vector{lftmatch}()
        major_codes = Vector{String}()
        push!(lftdata2d, lftdatavec)
        push!(lftmatch2d, lftmatchvec)
        push!(symbols2d, symbols)
        push!(datetimes2d, datetimes)
        push!(margin_ratio2d, margin_ratio)
        push!(price_tick2d, price_tick)
        push!(multiplier2d, multiplier) 
        push!(settleprice2d, settleprice)
        push!(major_codes2d, major_codes)
    end
    for i in eachindex(dates)
        futureticks = FuturesTick[]
        futureitems = HDataItem[]
        margin_ratio = margin_ratio2d[i]
        price_tick = price_tick2d[i]
        multiplier = multiplier2d[i]
        symbols = symbols2d[i]
        datetimes = datetimes2d[i]
        lftdatavec = lftdata2d[i]
        lftmatchvec = lftmatch2d[i]
        settleprice = settleprice2d[i]
        major_codes = major_codes2d[i]
        products = products_vec[i]
        codes = codes_vec[i]
        datei = datesint[i]
        filesi = [string("marketdata/tick_", datei)]
        if i > 1
            night_day = dates[i-1] + Day(1)
            if night_day != dates[i]
                night_file = string("marketdata/tick_", Dates.format(night_day,"yyyymmdd"))
                if isfile(string(folder,"/",night_file,".hdat"))
                    pushfirst!(filesi, night_file)
                end
            end
            pre_major_codes = major_codes2d[i-1]
            for pre_major_code in pre_major_codes
                margin_ratio[pre_major_code] = margin_ratio2d[i-1][pre_major_code]
                price_tick[pre_major_code] = price_tick2d[i-1][pre_major_code]
                multiplier[pre_major_code] = multiplier2d[i-1][pre_major_code]
            end
        else
            pre_major_codes = Vector{String}()
        end
        file_id = hdb_open_file(db_id, filesi[end], flags)[1]
        if length(products) != 0
            load_major_codeinfo!(file_id, margin_ratio, price_tick, multiplier, major_codes, products)
        end
        load_codeinfo!(file_id, margin_ratio, price_tick, multiplier, codes)
        part_symbols = union(major_codes, pre_major_codes)
        part_symbols = union(part_symbols, codes)
        if length(part_symbols) != 0
            high = Dict{String,Int}()
            low = Dict{String,Int}()
            if length(filesi) > 2
                file_id_night = hdb_open_file(db_id, filesi[1], flags)[1]
                if mode == 1
                    load_futureticks!(file_id_night, futureitems, futureticks, part_symbols)
                elseif mode == 2
                    load_futureticks!(file_id_night, futureitems, futureticks, part_symbols, datei)
                end  
            end
            if mode == 1
                load_futureticks!(file_id, futureitems, futureticks, part_symbols)
            elseif mode == 2
                load_futureticks!(file_id, futureitems, futureticks, part_symbols, datei)
            end            
            psymbols = parse_symbols(futureitems)
            globalvar = param_type()
            for j in eachindex(futureticks)
                tick = futureticks[j]
                symbol = psymbols[j]
                nowdt = parse_datetime(futureitems[j])
                dt = CTime(nowdt[2], tick.time)
                if 210000000 > nowdt[2] > 150000000
                    if abs(dt) > 3600
                        continue
                    end
                else
                    if abs(dt) > 60*3
                        continue
                    end                
                end                 
                if tick.settle_price > 0
                    settleprice[symbol] = tick.settle_price
                end
                if tick.match != 0
                    if symbol in keys(high)
                        high[symbol] = max(tick.match, high[symbol])
                        low[symbol] = min(tick.match, low[symbol])
                    else
                        high[symbol] = tick.match
                        low[symbol] = tick.match
                    end
                end
                res = generate(datei, symbol, nowdt, tick, globalvar)
                if isnothing(res)
                else
                    push!(lftmatchvec,lftmatch(high[symbol],low[symbol],tick.ask_price[1],
                            tick.ask_vol[1],tick.bid_price[1],tick.bid_vol[1]))
                    push!(lftdatavec,res)
                    push!(symbols, symbol)
                    push!(datetimes, nowdt)                    
                    pop!(high,symbol)
                    pop!(low,symbol)
                end
            end
        end
        hdb_close_file(file_id)
        if length(filesi) > 2
            hdb_close_file(file_id_night)
        end
        write(io, string(filesi[end]," has loaded\n"))
    end
    hdb_close_db(db_id)
    return lftdata2d
end

function load_lftdata(dates::Vector{Date}, products::Vector{String}, generate::Function,
        lftdata::DataType, param_type::DataType, io::IO, mode::Int)
    datesint = parse.(Int, Dates.format.(dates,"yyyymmdd"))
    global lftmatch2d, symbols2d, datetimes2d, margin_ratio2d, price_tick2d, multiplier2d
    global settleprice2d, major_codes2d, folder    
    db_id = hdb_open_db(folder)
    lftdata2d = Vector{Vector{lftdata}}()
    for i in eachindex(dates)
        lftdatavec = Vector{lftdata}()
        margin_ratio = Dict{String,NTuple{2,Integer}}()
        symbols = Vector{String}()
        datetimes = Vector{NTuple{2,Integer}}()
        price_tick = Dict{String, Integer}()
        multiplier = Dict{String,Integer}()
        settleprice = Dict{String,Integer}()
        lftmatchvec = Vector{lftmatch}()
        major_codes = Vector{String}()
        push!(lftdata2d, lftdatavec)
        push!(lftmatch2d, lftmatchvec)
        push!(symbols2d, symbols)
        push!(datetimes2d, datetimes)
        push!(margin_ratio2d, margin_ratio)
        push!(price_tick2d, price_tick)
        push!(multiplier2d, multiplier) 
        push!(settleprice2d, settleprice)
        push!(major_codes2d, major_codes)
    end
    for i in eachindex(dates)
        futureticks = FuturesTick[]
        futureitems = HDataItem[]
        margin_ratio = margin_ratio2d[i]
        price_tick = price_tick2d[i]
        multiplier = multiplier2d[i]
        symbols = symbols2d[i]
        datetimes = datetimes2d[i]
        lftdatavec = lftdata2d[i]
        lftmatchvec = lftmatch2d[i]
        settleprice = settleprice2d[i]
        major_codes = major_codes2d[i]
        datei = datesint[i]
        filesi = [string("marketdata/tick_", datei)]
        if i > 1
            night_day = dates[i-1] + Day(1)
            if night_day != dates[i]
                night_file = string("marketdata/tick_", Dates.format(night_day,"yyyymmdd"))
                if isfile(string(folder,"/",night_file,".hdat"))
                    pushfirst!(filesi, night_file)
                end
            end
            pre_major_codes = major_codes2d[i-1]
            for pre_major_code in pre_major_codes
                margin_ratio[pre_major_code] = margin_ratio2d[i-1][pre_major_code]
                price_tick[pre_major_code] = price_tick2d[i-1][pre_major_code]
                multiplier[pre_major_code] = multiplier2d[i-1][pre_major_code]
            end
        else
            pre_major_codes = Vector{String}()
        end
        file_id = hdb_open_file(db_id, filesi[end], flags)[1]
        if length(products) != 0
            load_major_codeinfo!(file_id, margin_ratio, price_tick, multiplier, major_codes, products)
        end
        part_symbols = union(major_codes, pre_major_codes)
        if length(part_symbols) != 0
            high = Dict{String,Int}()
            low = Dict{String,Int}()
            if length(filesi) > 2
                file_id_night = hdb_open_file(db_id, filesi[1], flags)[1]
                if mode == 1
                    load_futureticks!(file_id_night, futureitems, futureticks, part_symbols)
                elseif mode == 2
                    load_futureticks!(file_id_night, futureitems, futureticks, part_symbols, datei)
                end  
            end
            if mode == 1
                load_futureticks!(file_id, futureitems, futureticks, part_symbols)
            elseif mode == 2
                load_futureticks!(file_id, futureitems, futureticks, part_symbols, datei)
            end            
            psymbols = parse_symbols(futureitems)
            globalvar = param_type()
            for j in eachindex(futureticks)
                tick = futureticks[j]
                symbol = psymbols[j]
                nowdt = parse_datetime(futureitems[j])
                dt = CTime(nowdt[2], tick.time)
                if 210000000 > nowdt[2] > 150000000
                    if abs(dt) > 3600
                        continue
                    end
                else
                    if abs(dt) > 60*3
                        continue
                    end                
                end                 
                if tick.settle_price > 0
                    settleprice[symbol] = tick.settle_price
                end
                if tick.match != 0
                    if symbol in keys(high)
                        high[symbol] = max(tick.match, high[symbol])
                        low[symbol] = min(tick.match, low[symbol])
                    else
                        high[symbol] = tick.match
                        low[symbol] = tick.match
                    end
                end
                res = generate(datei, symbol, nowdt, tick, globalvar)
                if isnothing(res)
                else
                    push!(lftmatchvec,lftmatch(high[symbol],low[symbol],tick.ask_price[1],
                            tick.ask_vol[1],tick.bid_price[1],tick.bid_vol[1]))
                    push!(lftdatavec,res)
                    push!(symbols, symbol)
                    push!(datetimes, nowdt)                    
                    pop!(high,symbol)
                    pop!(low,symbol)
                end
            end
        end
        hdb_close_file(file_id)
        if length(filesi) > 2
            hdb_close_file(file_id_night)
        end
        write(io, string(filesi[end]," has loaded\n"))
    end
    hdb_close_db(db_id)
    return lftdata2d
end

function load_lftdata_security(dates::Vector{Date}, codes::Vector{String}, generate::Function,
         lftdata::DataType, param_type::DataType, io::IO)
    datesint = parse.(Int, Dates.format.(dates,"yyyymmdd"))
    files = string.("marketdata/tick_",datesint)
    global lftmatch2d, symbols2d, datetimes2d, margin_ratio2d, price_tick2d, multiplier2d
    global settleprice2d, major_codes2d, folder    
    db_id = hdb_open_db(folder)
    lftdata2d = Vector{Vector{lftdata}}()
    for i in eachindex(files)
        lftdatavec = Vector{lftdata}()
        margin_ratio = Dict{String,NTuple{2,Integer}}()
        symbols = Vector{String}()
        datetimes = Vector{NTuple{2,Integer}}()
        price_tick = Dict{String, Integer}()
        multiplier = Dict{String,Integer}()
        settleprice = Dict{String,Integer}()
        lftmatchvec = Vector{lftmatch}()
        push!(lftdata2d, lftdatavec)
        push!(lftmatch2d, lftmatchvec)
        push!(symbols2d, symbols)
        push!(datetimes2d, datetimes)
        push!(margin_ratio2d, margin_ratio)
        push!(price_tick2d, price_tick)
        push!(multiplier2d, multiplier) 
        push!(settleprice2d, settleprice)
        push!(major_codes2d, codes)
    end
    for i in eachindex(files)
        securityticks = SecurityTick[]
        futureitems = HDataItem[]
        margin_ratio = margin_ratio2d[i]
        price_tick = price_tick2d[i]
        multiplier = multiplier2d[i]
        symbols = symbols2d[i]
        datetimes = datetimes2d[i]
        lftdatavec = lftdata2d[i]
        lftmatchvec = lftmatch2d[i]
        settleprice = settleprice2d[i]
        file = files[i]
        datei = datesint[i]
        file_id = hdb_open_file(db_id, file, flags)[1]
        for code in codes
            margin_ratio[code] = (10000, 10000)
            price_tick[code] = 100
            multiplier[code] = 100
        end
        part_symbols = Vector{String}()
        push!(part_symbols, keys(margin_ratio)...)
        if length(part_symbols) != 0
            high = Dict{String,Int}()
            low = Dict{String,Int}()
            load_securityticks!(file_id, futureitems, securityticks, part_symbols)
            psymbols = parse_symbols(futureitems)
            globalvar = param_type()
            for j in eachindex(securityticks)
                tick = securityticks[j]
                symbol = psymbols[j]
                nowdt = parse_datetime(futureitems[j])
                if tick.match > 0
                    settleprice[symbol] = tick.match
                end
                if tick.match != 0
                    if symbol in keys(high)
                        high[symbol] = max(tick.match, high[symbol])
                        low[symbol] = min(tick.match, low[symbol])
                    else
                        high[symbol] = tick.match
                        low[symbol] = tick.match
                    end
                end
                res = generate(symbol, nowdt, tick, globalvar)
                if isnothing(res)
                else
                    push!(lftmatchvec,lftmatch(high[symbol],low[symbol],tick.ask_price[1],
                          tick.ask_vol[1],tick.bid_price[1],tick.bid_vol[1]))
                    push!(lftdatavec,res)
                    push!(symbols, symbol)
                    push!(datetimes, nowdt)                    
                    pop!(high,symbol)
                    pop!(low,symbol)
                end
            end
        end
        hdb_close_file(file_id)
        write(io, string(file," has loaded\n"))
    end
    hdb_close_db(db_id)
    return lftdata2d
end

function generatekline(symbol::String, nowdt::NTuple{2,Integer}, ftick::SecurityTick, globalvar::dataparam)
    if (ftick.time == 0) || (sum(ftick.ask_vol+ftick.bid_vol)==0) || (ftick.match == 0)
        return nothing
    end
    if symbol in keys(globalvar.last_timed)
        last_time = globalvar.last_timed[symbol]
        last_volume = globalvar.last_volume[symbol]
        last_turnover = globalvar.last_turnover[symbol]
        pre_close = globalvar.pre_close[symbol]
    else
        globalvar.last_timed[symbol] = 93000000
        last_time = 93000000
        globalvar.last_volume[symbol] = 0
        last_volume = 0
        globalvar.last_turnover[symbol] = 0
        last_turnover = 0
        globalvar.oneminute_prices[symbol] = Vector{Int64}()
        globalvar.pre_close[symbol] = ftick.pre_close
        pre_close = ftick.pre_close
    end
    exchange = split(symbol, ".")[1]
    timei = ftick.time
    dt = CFFEXMTime(timei, last_time)
    if ftick.turnover - last_turnover < 0
        return nothing
    end
    push!(globalvar.oneminute_prices[symbol], ftick.match)
    product = getproduct(symbol)
    if product in ["CFFEX.T","CFFEX.TS","CFFEX.TF"]
        time1 = 151500000
        time2 = 151440000
    else
        time1 = 150000000
        time2 = 145940000
    end
    if (dt >= 60) || (time1 >= timei > time2)
        kopen = globalvar.oneminute_prices[symbol][1]
        kclose = globalvar.oneminute_prices[symbol][end]
        khigh = maximum(globalvar.oneminute_prices[symbol])
        klow = minimum(globalvar.oneminute_prices[symbol])
        kvolume = ftick.volume - last_volume
        kturnover = ftick.turnover - last_turnover
        klinetick = kline(ftick, kopen, khigh, klow, kclose, kvolume, kturnover, Int64(pre_close))
        globalvar.last_timed[symbol] = timei
        globalvar.last_volume[symbol] = ftick.volume
        globalvar.last_turnover[symbol] = ftick.turnover
        globalvar.pre_close[symbol] = kclose
        empty!(globalvar.oneminute_prices[symbol])
        return klinetick
    end
    return nothing
end

function generatekline_night(tradeday::Integer, symbol::String, nowdt::NTuple{2,Integer}, ftick::FuturesTick, globalvar::dataparam)
    if (ftick.time == 0) || (sum(ftick.ask_vol+ftick.bid_vol)==0) || (ftick.match == 0)
        return nothing
    end
    if symbol in keys(globalvar.last_timed)
        last_tradeday = globalvar.last_tradeday[symbol]
        last_date = globalvar.last_date[symbol]
        last_time = globalvar.last_timed[symbol]
        last_volume = globalvar.last_volume[symbol]
        last_turnover = globalvar.last_turnover[symbol]
        pre_close = globalvar.pre_close[symbol]
    else
        exchange = split(symbol, ".")[1]
        globalvar.last_tradeday[symbol] = tradeday
        last_tradeday = tradeday
        globalvar.last_date[symbol] = nowdt[1]
        last_date = nowdt[1]
        if exchange in ["CFFEX","SH","SZ"]
            globalvar.last_timed[symbol] = 93000000
            last_time = 93000000
        else
            globalvar.last_timed[symbol] = 90000000
            last_time = 90000000
        end
        globalvar.last_volume[symbol] = 0
        last_volume = 0
        globalvar.last_turnover[symbol] = 0
        last_turnover = 0
        globalvar.oneminute_prices[symbol] = Vector{Int64}()
        globalvar.pre_close[symbol] = ftick.pre_close
        pre_close = ftick.pre_close
    end
    product = getproduct(symbol)
    if product in ["CFFEX.T","CFFEX.TS","CFFEX.TF","CFFEX.TL"]
        time1 = 151500000
        time2 = 151440000
    else
        time1 = 150000000
        time2 = 145940000
    end    
    if last_tradeday < tradeday
       last_turnover = 0
       last_volume = 0
    end
    if ftick.turnover < last_turnover
        return nothing
    end
    timei = ftick.time
    dt = MDateTime(product, nowdt[1], timei, last_date, last_time)    
    push!(globalvar.oneminute_prices[symbol], ftick.match)
    if (dt >= 60) || (time1 >= timei > time2)
        kopen = globalvar.oneminute_prices[symbol][1]
        kclose = globalvar.oneminute_prices[symbol][end]
        khigh = maximum(globalvar.oneminute_prices[symbol])
        klow = minimum(globalvar.oneminute_prices[symbol])
        kvolume = ftick.volume - last_volume
        kturnover = ftick.turnover - last_turnover
        klinetick = kline(ftick, kopen, khigh, klow, kclose, kvolume, kturnover, pre_close)
        globalvar.last_date[symbol] = nowdt[1]
        globalvar.last_tradeday[symbol] = tradeday
        globalvar.last_timed[symbol] = timei
        globalvar.last_volume[symbol] = ftick.volume
        globalvar.last_turnover[symbol] = ftick.turnover
        globalvar.pre_close[symbol] = kclose
        empty!(globalvar.oneminute_prices[symbol])
        return klinetick
    end
    return nothing
end

function load_minkline(dates::Vector{Date}, products::Vector{String}; instrument="future")
    logfile = string(logdir, Dates.format(Dates.now(),"yyyymmdd_"),myid(),".log")
    io = open(logfile,"a")
    if instrument == "future"
        global custom2d = load_lftdata(dates, products, generatekline_night, kline, dataparam, io, 1)
   #    global custom2d = load_lftdata_multi(dates, products, generatekline, kline, dataparam, io, 1)
    elseif instrument == "security"
        global custom2d = load_lftdata_security(dates, products, generatekline, kline, dataparam, io)
    end
    close(io)
    nothing
end

function load_minkline(dates::Vector{Date}, products_vec::Vector{Vector{String}},
        codes_vec::Vector{Vector{String}})
    logfile = string(logdir, Dates.format(Dates.now(),"yyyymmdd_"),myid(),".log")
    io = open(logfile,"a")
    global custom2d = load_lftdata(dates, products_vec, codes_vec, generatekline_night, kline, dataparam, io, 1)
#    global custom2d = load_lftdata_multi(dates, products, generatekline, kline, dataparam, io, 1)
    close(io)
    nothing
end

function generateTline(symbol::String, nowdt::NTuple{2,Integer}, ftick::FuturesTick, globalvar::dataparam)
    if (ftick.time == 0) || (sum(ftick.ask_vol+ftick.bid_vol)==0) || (ftick.match == 0)
        return nothing
    end
    if symbol in keys(globalvar.last_timed)
        last_time = globalvar.last_timed[symbol]
        last_volume = globalvar.last_volume[symbol]
        last_turnover = globalvar.last_turnover[symbol]
		pre_close = globalvar.pre_close[symbol]
    else
        globalvar.last_timed[symbol] = 90000000
        last_time = 90000000
        globalvar.volume[symbol] = 0
        last_volume = 0
        globalvar.turnover[symbol] = 0
        last_turnover = 0
        globalvar.oneminute_prices[symbol] = Vector{Int64}()
		globalvar.pre_close[symbol] = ftick.pre_close
		pre_close = ftick.pre_close
    end
    exchange = split(symbol, ".")[1]
    timei = ftick.time
    if exchange in ["CFFEX"]
        dt = CFFEXMTime(timei, last_time)
    else
        dt = MTime(timei, last_time)
    end
    if ftick.turnover - last_turnover < 0
        return nothing
    end
    push!(globalvar.oneminute_prices[symbol], ftick.match)
    product = getproduct(symbol)
    if product in ["CFFEX.T","CFFEX.TS","CFFEX.TF"]
        time1 = 151500000
        time2 = 151440000
    else
        time1 = 150000000
        time2 = 145940000
    end
    if (dt >= 60) || (time1 >= timei > time2)
        kopen = ftick.open
        kclose = globalvar.oneminute_prices[symbol][end]
        khigh = maximum(globalvar.oneminute_prices[symbol])
        klow = minimum(globalvar.oneminute_prices[symbol])
        kvolume = ftick.volume - last_volume
        kturnover = ftick.turnover - last_turnover
        klinetick = kline(ftick, kopen, khigh, klow, kclose, kvolume, kturnover, pre_close)
        globalvar.last_timed[symbol] = timei
        globalvar.last_volume[symbol] = ftick.volume
        globalvar.last_turnover[symbol] = ftick.turnover
		globalvar.pre_close[symbol] = kclose
        empty!(globalvar.oneminute_prices[symbol])
        return klinetick
    end
    return nothing    
end

function generateTline_night(tradeday::Integer, symbol::String, nowdt::NTuple{2,Integer}, ftick::FuturesTick, globalvar::dataparam)
    if (ftick.time == 0) || (sum(ftick.ask_vol+ftick.bid_vol)==0) || (ftick.match == 0)
        return nothing
    end
    if symbol in keys(globalvar.last_timed)
        last_tradeday = globalvar.last_tradeday[symbol]
        last_date = globalvar.last_date[symbol]    
        last_time = globalvar.last_timed[symbol]
        last_volume = globalvar.last_volume[symbol]
        last_turnover = globalvar.last_turnover[symbol]
		pre_close = globalvar.pre_close[symbol]
    else
        globalvar.last_tradeday[symbol] = tradeday
        last_tradeday = tradeday
        globalvar.last_date[symbol] = nowdt[1]
        last_date = nowdt[1]    
        globalvar.last_timed[symbol] = 90000000
        last_time = 90000000
        globalvar.volume[symbol] = 0
        last_volume = 0
        globalvar.turnover[symbol] = 0
        last_turnover = 0
        globalvar.oneminute_prices[symbol] = Vector{Int64}()
		globalvar.pre_close[symbol] = ftick.pre_close
		pre_close = ftick.pre_close
    end
    product = getproduct(symbol)
    if product in ["CFFEX.T","CFFEX.TS","CFFEX.TF","CFFEX.TL"]
        time1 = 151500000
        time2 = 151440000
    else
        time1 = 150000000
        time2 = 145940000
    end    
    if last_tradeday < tradeday
       last_turnover = 0
       last_volume = 0
    end    
    if ftick.turnover < last_turnover
        return nothing
    end
    timei = ftick.time
    dt = MDateTime(product, nowdt[1], timei, last_date, last_time)
    push!(globalvar.oneminute_prices[symbol], ftick.match)
    if (dt >= 60) || (time1 >= timei > time2)
        kopen = ftick.open
        kclose = globalvar.oneminute_prices[symbol][end]
        khigh = maximum(globalvar.oneminute_prices[symbol])
        klow = minimum(globalvar.oneminute_prices[symbol])
        kvolume = ftick.volume - last_volume
        kturnover = ftick.turnover - last_turnover
        klinetick = kline(ftick, kopen, khigh, klow, kclose, kvolume, kturnover, pre_close)
        globalvar.last_date[symbol] = nowdt[1]
        globalvar.last_tradeday[symbol] = tradeday        
        globalvar.last_timed[symbol] = timei
        globalvar.last_volume[symbol] = ftick.volume
        globalvar.last_turnover[symbol] = ftick.turnover
		globalvar.pre_close[symbol] = kclose
        empty!(globalvar.oneminute_prices[symbol])
        return klinetick
    end
    return nothing    
end

function load_Tline(dates::Vector{Date}, products::Vector{String})
    logfile = string(logdir, Dates.format(Dates.now(),"yyyymmdd_"),myid(),".log")
    io = open(logfile,"w")
#    global custom2d = load_lftdata_multi(dates, products, generateTline, kline, dataparam, io, 2)
    global custom2d = load_lftdata(dates, products, generateTline_night, kline, dataparam, io, 2)
    close(io)
    nothing
end

function load_Tline(dates::Vector{Date}, products_vec::Vector{Vector{String}},
                    codes_vec::Vector{Vector{String}})
    logfile = string(logdir, Dates.format(Dates.now(),"yyyymmdd_"),myid(),".log")
    io = open(logfile,"w")
#    global custom2d = load_lftdata_multi(dates, products, generateTline, kline, dataparam, io, 2)
    global custom2d = load_lftdata(dates, products_vec, codes_vec, generateTline_night, kline, dataparam, io, 2)
    close(io)
    nothing
end

function get_all_product(dates::Vector{Date})
    global folder
    datesint = parse.(Int, Dates.format.(dates,"yyyymmdd"))
    files = string.("marketdata/tick_",datesint)
    products = Set{String}()
    flags = 0
    db_id = hdb_open_db(folder)
    Threads.@threads for i in eachindex(files)
        file = files[i]
        file_id = hdb_open_file(db_id, file, flags)[1]
        items = hdb_read_codetable(file_id, "ZLHY")
        symbols = getproduct.(parse_symbols(items))
        push!(products,symbols...)
        hdb_close_file(file_id)
        println(file, " has completed")
    end
    hdb_close_db(db_id)
    return [products...]
end

function get_all_product()
    global dates,folder
    datesint = parse.(Int, Dates.format.(dates,"yyyymmdd"))
    files = string.("marketdata/tick_",datesint)
    products = Set{String}()
    flags = 0
    db_id = hdb_open_db(folder)
    for i in eachindex(files)
        file = files[i]
        file_id = hdb_open_file(db_id, file, flags)[1]
        items = hdb_read_codetable(file_id, "ZLHY")
        symbols = getproduct.(parse_symbols(items))
        if length(symbols) != 0
            push!(products,symbols...)
        end
        hdb_close_file(file_id)
        println(file, " has completed")
    end
    hdb_close_db(db_id)
    return [products...]
end
export get_all_product

global on_custom::Function
function md_set_custom_callback(on_custom_func::Function, custom_t::DataType, custom_p::DataType)
    global on_custom = on_custom_func
    global custom_type = custom_t
    global custom_param = custom_p
end
export md_set_custom_callback

#function load_custom_multi(dates::Vector{Date}, products::Vector{String})
#    global on_day_start, on_custom, custom_type, custom_param
#    logfile = string(logdir, Dates.format(Dates.now(),"yyyymmdd_"),myid(),".log")
#    io = open(logfile,"w")
#    global custom2d = load_lftdata_multi(dates, products, on_day_start, on_custom, custom_type, custom_param, io, 1)
#    close(io)
#    nothing
#end

function load_custom(dates::Vector{Date}, products::Vector{String})
    global on_custom, custom_type, custom_param
    logfile = string(logdir, Dates.format(Dates.now(),"yyyymmdd_"),myid(),".log")
    io = open(logfile,"a")
    global custom2d = load_lftdata(dates, products, on_custom, custom_type, custom_param, io, 1)
    close(io)
    nothing
end

function load_custom(dates::Vector{Date}, products_vec::Vector{Vector{String}},
                     codes_vec::Vector{Vector{String}})
    global on_custom, custom_type, custom_param
    logfile = string(logdir, Dates.format(Dates.now(),"yyyymmdd_"),myid(),".log")
    io = open(logfile,"a")
    global custom2d = load_lftdata(dates, products_vec, codes_vec, on_custom, custom_type, custom_param, io, 1)
    close(io)
    nothing
end

function loadhistorydata(products::Vector{String}, linetype::String; instrument="future")
    global dates
    data_reset()
    setdates()    
    if linetype == "custom"
        load_custom(dates, products)
#    elseif linetype == "custom_async"
#        load_custom_multi(dates, products)
    elseif linetype == "minkline"
        load_minkline(dates, products; instrument=instrument)
    elseif linetype == "tline"
        load_Tline(dates, products)
    end
    nothing
end
export loadhistorydata

function loadhistorydata(products_vec::Vector{Vector{String}}, codes_vec::Vector{Vector{String}}, linetype::String)
    global dates
    data_reset()
    setdates()
    if linetype == "custom"
        load_custom(dates, products_vec, codes_vec)
#    elseif linetype == "custom_async"
#        load_custom_multi(dates, products_vec, codes_vec)
    elseif linetype == "minkline"
        load_minkline(dates, products_vec, codes_vec)
    elseif linetype == "tline"
        load_Tline(dates, products_vec, codes_vec)
    end
    nothing
end
export loadhistorydata
######################################################################################################
#                                          order
######################################################################################################
mutable struct sysparam
    strategy_name::String
    params::String
    last_tick::Dict{String,Tuple{Integer,Integer}}
    day_schedule_times::Vector{Integer}
    timepoint_index::Int
#    ordertrace_orderlist::Vector{Dict{String, Any}}
    ordertrace_orderlist::Dict{String, Vector}
    ordertrace_last_orderitem::Dict{String,Dict{String,Any}}
    ordertrace_retracement::Dict{String,Number}
    ordertrace_last_netvalue::Dict{String,Number}
    ordertrace_max_netvalue::Dict{String,Number}
    ordertrace_margin_ratio::Dict{String,NTuple{2,Integer}}
    ordertrace_price_tick::Dict{String,Integer}
    ordertrace_multiplier::Dict{String,Integer}
    ordertrace_fee::Dict{String,Tuple{Int8,Float32,Float32,Float32,Float32}}
    ordertrace_settleprice::Dict{String, Integer}
    ordertrace_major_codes::Vector{String}
    ordertrace_tradeday::Integer
end

function sysparam(strategy_name::String, params::String)
    last_tick = Dict{String,Tuple{Integer,Integer}}()
    day_schedule_times = Vector{Integer}()
    push!(day_schedule_times, 250000)
    timepoint_index = 1
    ordertrace_orderlist = Dict{String, Vector}()
    ordertrace_last_orderitem = Dict{String,Dict{String,Any}}()
    ordertrace_retracement = Dict{String,Number}()
    ordertrace_last_netvalue = Dict{String,Number}()
    ordertrace_max_netvalue = Dict{String,Number}()
    ordertrace_margin_ratio = Dict{String,NTuple{2,Integer}}()
    ordertrace_price_tick = Dict{String,Integer}()
    ordertrace_multiplier = Dict{String,Integer}()
    ordertrace_fee = Dict{String,Tuple{Int8,Float32,Float32,Float32,Float32}}()
    ordertrace_settleprice = Dict{String, Integer}()
    ordertrace_major_codes = Vector{String}()
    ordertrace_tradeday = 0
    sysparam(strategy_name, params, last_tick, day_schedule_times, timepoint_index, 
    ordertrace_orderlist, ordertrace_last_orderitem, 
    ordertrace_retracement, ordertrace_last_netvalue, ordertrace_max_netvalue, 
    ordertrace_margin_ratio, ordertrace_price_tick,ordertrace_multiplier, ordertrace_fee, 
    ordertrace_settleprice, ordertrace_major_codes, 0)
end

function reset_sysdata!(sys_data::sysparam)
    empty!(sys_data.last_tick)
    empty!(sys_data.day_schedule_times)
    push!(sys_data.day_schedule_times, 250000)
    sys_data.timepoint_index = 1
    empty!(sys_data.ordertrace_orderlist)
    empty!(sys_data.ordertrace_last_orderitem)
    empty!(sys_data.ordertrace_retracement)
    empty!(sys_data.ordertrace_last_netvalue)
    empty!(sys_data.ordertrace_max_netvalue)
#    empty!(sys_data.ordertrace_margin_ratio)
#    empty!(sys_data.ordertrace_price_tick)
#    empty!(sys_data.ordertrace_multiplier)
    empty!(sys_data.ordertrace_fee)  
#    empty!(sys_data.ordertrace_settleprice)   
end

export sysparam
global enginedata = Vector{sysparam}()
#####################################################################################
#  order_trace
#########################################################################################
#orderitem = ['datetime_open','symbol','price_open','datetime_close','price_close','trade_side','margin_ratio',
#'product']

function ordertrace_generate_orderlist!(ordertrace_orderlist::Dict{String, Vector})
    ordertrace_orderlist["price_close"] = Vector{Int64}()
    ordertrace_orderlist["price_open"] = Vector{Int64}()
    ordertrace_orderlist["date_open"] = Vector{Int32}()
    ordertrace_orderlist["date_close"] = Vector{Int32}()
    ordertrace_orderlist["time_open"] = Vector{Int32}()
    ordertrace_orderlist["time_close"] = Vector{Int32}()
    ordertrace_orderlist["symbol"] = Vector{String}()
    ordertrace_orderlist["return_margin"] = Vector{Float64}()
    ordertrace_orderlist["margin_ratio"] = Vector{Int64}()
    ordertrace_orderlist["product"] = Vector{String}()
    ordertrace_orderlist["trade_side"] = Vector{String}()
    ordertrace_orderlist["return"] = Vector{Float64}()
    ordertrace_orderlist["price_tick"] = Vector{Int64}()
    ordertrace_orderlist["multiplier"] = Vector{Int64}()
    ordertrace_orderlist["basepoint"] = Vector{Float64}()
    ordertrace_orderlist["fee"] = Vector{Float64}()
    ordertrace_orderlist["islock"] = Vector{Int16}()
    ordertrace_orderlist["return_worst"] = Vector{Float64}()  
    ordertrace_orderlist["return_worst_margin"] = Vector{Float64}()    
    ordertrace_orderlist["return_best"] = Vector{Float64}()  
    ordertrace_orderlist["return_best_margin"] = Vector{Float64}()       
    ordertrace_orderlist["volume"] = Vector{Int64}()    
end
  
function ordertrace_init!(sys_data::sysparam, margin_ratio::Dict{String,NTuple{2,Integer}}, price_tick::Dict{String,Integer},
                          multiplier::Dict{String,Integer}, settleprice::Dict{String, Integer},
                          major_codes::Vector{String},tradeday::Integer)
    sys_data.ordertrace_margin_ratio = margin_ratio
    sys_data.ordertrace_price_tick = price_tick
    sys_data.ordertrace_multiplier = multiplier
    sys_data.ordertrace_settleprice = settleprice
    sys_data.ordertrace_major_codes = major_codes
    sys_data.ordertrace_tradeday = tradeday
end
export ordertrace_init!

function ordertrace_load!(sys_data::sysparam)
    global orderfee_file
    ordertrace_fee = sys_data.ordertrace_fee
    content = readlines(orderfee_file)
#product, symbol, type, open_today, open_preday, close_today, close_preday
    for line in content[2:end]
        words = split(line,",")
        feetype = parse(Int, words[3])
        open_today = parse(Float32, words[4])
        open_preday = parse(Float32, words[5])
        close_today = parse(Float32, words[6])
        close_preday = parse(Float32, words[7])
        if length(strip(words[2])) != 0
            ordertrace_fee[strip(words[2])] = (feetype, open_today, open_preday, close_today, close_preday)
        else
            ordertrace_fee[strip(words[1])] = (feetype, open_today, open_preday, close_today, close_preday)
        end
    end
    ordertrace_generate_orderlist!(sys_data.ordertrace_orderlist)
end

function ordertrace_order_handle(sys_data::sysparam, symbol::String, price::Number, side::String,
                                datetimeo::NTuple{2,Integer}, product::String, settle_flag::Int)
    ordertrace_last_orderitem = sys_data.ordertrace_last_orderitem
    ordertrace_orderlist = sys_data.ordertrace_orderlist
    ordertrace_margin_ratio = sys_data.ordertrace_margin_ratio
    ordertrace_price_tick = sys_data.ordertrace_price_tick
    ordertrace_multiplier = sys_data.ordertrace_multiplier
    if !(symbol in keys(ordertrace_last_orderitem))
        ordertrace_last_orderitem[symbol] = Dict{String,Any}()
    end
    orderitem = ordertrace_last_orderitem[symbol]
    ocdict = Dict{String,String}("long_close"=>"close","short_close"=>"close","short_open"=>"open",
                                 "long_open"=>"open")
    tsidedict = Dict{String,String}("long_close"=>"Long","short_close"=>"Short","short_open"=>"Short",
                                 "long_open"=>"Long")
    openclose = ocdict[side]
    trade_side = tsidedict[side]
    if openclose == "open"
        if "price_open" in keys(orderitem)
            return -1
        else
            orderitem["date_open"] = Int32(datetimeo[1])
            orderitem["time_open"] = Int32(datetimeo[2])
            orderitem["price_open"] = Int64(price)
            orderitem["trade_side"] = trade_side
            orderitem["margin_ratio"] = Int64(ordertrace_margin_ratio[symbol][1])
            orderitem["price_tick"] = Int64(ordertrace_price_tick[symbol])
            orderitem["multiplier"] = Int64(ordertrace_multiplier[symbol])
            orderitem["product"] = product
            if settle_flag == 0
                feetuple = ordertrace_fee(sys_data, symbol, orderitem["multiplier"]*orderitem["price_open"], 1, 2)
                orderitem["fee"] = feetuple[1]
                orderitem["islock"] = Int16(feetuple[3])
                orderitem["positiontype"] = "new"
            elseif settle_flag == 1
                orderitem["fee"] = 0
                orderitem["islock"] = 0
                orderitem["positiontype"] = "old"
            end
            orderitem["worstprice"] = Int64(price)
            orderitem["bestprice"] = Int64(price)
            return 0
        end  
    elseif openclose == "close"
        if ("trade_side" in keys(orderitem)) && ("price_open" in keys(orderitem))
            if orderitem["trade_side"] == trade_side
                orderitem["date_close"] = Int32(datetimeo[1])
                orderitem["time_close"] = Int32(datetimeo[2])
                orderitem["price_close"] = Int64(price)
                orderitem["trade_side"] = trade_side
                if settle_flag == 0
                    fee = 0
                    if orderitem["positiontype"] == "new"
                        feetuple = ordertrace_fee(sys_data, symbol, orderitem["multiplier"]*orderitem["price_close"], 1, 2)
                        orderitem["fee"] = Float64(orderitem["fee"] + feetuple[2])
                        orderitem["islock"] = Int16(feetuple[3])
                    elseif orderitem["positiontype"] == "old"
                        #1表示长线开平仓手续费
                        feetuple = ordertrace_fee(sys_data, symbol, orderitem["multiplier"]*orderitem["price_close"], 1, 1)
                        orderitem["fee"] = Float64(feetuple[2])
                        orderitem["islock"] = Int16(feetuple[3])                        
                    end
                elseif settle_flag == 1
                    #1表示长线开平仓手续费
                    feetuple = ordertrace_fee(sys_data, symbol, orderitem["multiplier"]*orderitem["price_close"], 1, 1)
                    if orderitem["positiontype"] == "new"
                        orderitem["fee"] = Float64(feetuple[1])
                        orderitem["islock"] = Int16(feetuple[3])
                    end
                end
                if trade_side == "Short"
                    orderitem["basepoint"] = Float64((orderitem["price_open"]-orderitem["price_close"]-
                                        orderitem["fee"]/orderitem["multiplier"])/orderitem["price_tick"])
                    orderitem["worstprice"] = max(orderitem["worstprice"], Int64(price))
                    orderitem["bestprice"] = min(orderitem["bestprice"], Int64(price))
                    orderitem["return"] = Float64(1 - (orderitem["price_close"]*orderitem["multiplier"]+orderitem["fee"]
                                        )/(orderitem["price_open"]*orderitem["multiplier"]))
                    orderitem["return_worst"] = Float64(1 - orderitem["worstprice"]/orderitem["price_open"])        
                    orderitem["return_best"] = Float64(1 - orderitem["bestprice"]/orderitem["price_open"])                    
                elseif trade_side == "Long"
                    orderitem["basepoint"] = Float64((orderitem["price_close"]-orderitem["price_open"]-
                                         orderitem["fee"]/orderitem["multiplier"])/orderitem["price_tick"])
                    orderitem["worstprice"] = min(orderitem["worstprice"], Int64(price))
                    orderitem["bestprice"] = max(orderitem["bestprice"], Int64(price))
                    orderitem["return"] = Float64((orderitem["price_close"]*orderitem["multiplier"]-orderitem["fee"]
                                    )/(orderitem["price_open"]*orderitem["multiplier"]) - 1)
                    orderitem["return_worst"] = Float64(orderitem["worstprice"]/orderitem["price_open"] - 1)
                    orderitem["return_best"] = Float64(orderitem["bestprice"]/orderitem["price_open"] - 1)
                else
                    println("trade_side error0")
                end
                orderitem["return_worst"] = min(orderitem["return"], orderitem["return_worst"])
                orderitem["return_best"] = max(orderitem["return"], orderitem["return_best"])
                orderitem["return_margin"] = Float64(10000*orderitem["return"]/orderitem["margin_ratio"])
                orderitem["return_worst_margin"] = Float64(10000*orderitem["return_worst"]/orderitem["margin_ratio"])
                orderitem["return_best_margin"] = Float64(10000*orderitem["return_best"]/orderitem["margin_ratio"])
                orderitem["symbol"] = symbol
                orderitem["volume"] = Int64(1)
                ordertrace_last_netvalue = sys_data.ordertrace_last_netvalue
                ordertrace_max_netvalue = sys_data.ordertrace_max_netvalue
                if product in keys(ordertrace_last_netvalue)
                    max_value = ordertrace_max_netvalue[product]
                    netvalue1 = ordertrace_last_netvalue[product] + orderitem["return_worst_margin"]
                    netvalue2 = ordertrace_last_netvalue[product] + orderitem["return_margin"]
                else
                    max_value = 1
                    netvalue1 = 1 + orderitem["return_worst_margin"]
                    netvalue2 = 1 + orderitem["return_margin"]
                end
                retracement1 = (netvalue1 - max(max_value, netvalue1))/max(max_value, netvalue1)
                retracement2 = (netvalue2 - max(max_value, netvalue1, netvalue2))/ max(max_value, netvalue1, netvalue2)
                sys_data.ordertrace_last_netvalue[product] = netvalue2
                sys_data.ordertrace_max_netvalue[product] = max(max_value, netvalue1, netvalue2)
                if product in keys(sys_data.ordertrace_retracement)
                    last_retracement = sys_data.ordertrace_retracement[product]
                    sys_data.ordertrace_retracement[product] = min(retracement1, retracement2, last_retracement)
                else
                    sys_data.ordertrace_retracement[product] = min(retracement1, retracement2)
                end
                for key in keys(sys_data.ordertrace_orderlist)
                    push!(sys_data.ordertrace_orderlist[key], orderitem[key])
                end
                pop!(ordertrace_last_orderitem, symbol)    
                return 0
            else
                return -2
            end
        else
            return -3
        end
    else
        return -4
    end
end

function ordertrace_setworstprice!(sys_data::sysparam, symbol::String, high::Int64, low::Int64)
    ordertrace_last_orderitem = sys_data.ordertrace_last_orderitem
    if symbol in keys(ordertrace_last_orderitem)
        orderitem = ordertrace_last_orderitem[symbol]
        if "trade_side" in keys(orderitem)
            if orderitem["trade_side"] == "Long"
                orderitem["worstprice"] = min(Int64(low),orderitem["worstprice"])
                orderitem["bestprice"] = max(Int64(high),orderitem["bestprice"])
            elseif orderitem["trade_side"] == "Short"
                orderitem["worstprice"] = max(Int64(high),orderitem["worstprice"])
                orderitem["bestprice"] = min(Int64(low),orderitem["bestprice"])
            end
        end
    end
end
export ordertrace_setworstprice!

function ordertrace_profit_return(sys_data::sysparam)
    params = sys_data.params
    ordertrace_orderlist = sys_data.ordertrace_orderlist
    ordertrace_retracement = sys_data.ordertrace_retracement
    pr = Dict{String,Number}()
    pc = Dict{String,Number}()
    pb = Dict{String,Number}()
    pp = Dict{String,Number}()
    len = length(ordertrace_orderlist["date_open"])
    for i in 1:len
        product = ordertrace_orderlist["product"][i]
        if product in keys(pr)
            pr[product] = pr[product] + ordertrace_orderlist["return_margin"][i]
            pc[product] = pc[product] + 1
            pb[product] = pb[product] + ordertrace_orderlist["basepoint"][i]
            pp[product] = pp[product] + ordertrace_orderlist["basepoint"][i]*ordertrace_orderlist["multiplier"][i]*ordertrace_orderlist["price_tick"][i]/10000
        else
            pr[product] = ordertrace_orderlist["return_margin"][i]
            pc[product] = 1
            pb[product] = ordertrace_orderlist["basepoint"][i]
            pp[product] = ordertrace_orderlist["basepoint"][i]*ordertrace_orderlist["multiplier"][i]*ordertrace_orderlist["price_tick"][i]/10000        
        end
    end
    result = Vector{Tuple}()
    for key in keys(pr)
        return_mean = pr[key]/pc[key]
        basepoint_per_trade = pb[key]/pc[key]
        trade_count = pc[key]
        product = key
        point = pp[key]
        restuple = (return_mean, ordertrace_retracement[key], trade_count, product,
                    point, basepoint_per_trade, params)
        push!(result, restuple)
    end
    return result
end

function ordertrace_fee(sys_data::sysparam, symbol::String, turnover::Number, volume::Integer, postype::Int)::Tuple{Float32,Float32,Int16}
#product, symbol, type, open_today, open_preday, close_today, close_preday
    product = getproduct(symbol)
    exchange = split(symbol,".")[1]
    if exchange in ["SH", "SZ"]
        global fee_security
        open_ = fee_security*turnover
        close_ = fee_security*turnover
        return (open_, close_, 0)
    end
    if symbol in keys(sys_data.ordertrace_fee)
        feetype = sys_data.ordertrace_fee[symbol][1]
        feelist = sys_data.ordertrace_fee[symbol][2:end]
    elseif product in keys(sys_data.ordertrace_fee)
        feetype = sys_data.ordertrace_fee[product][1]
        feelist = sys_data.ordertrace_fee[product][2:end]
    else 
        return (0,0,0)
    end
    if feetype == 2
        lockopen = 10000*feelist[2]*volume*2
        lockclose = 10000*feelist[4]*volume*2
        open_today = 10000*feelist[1]*volume
        close_today = 10000*feelist[3]*volume
        if postype == 1
            return (lockopen/2, lockclose/2, 0)
        else
            if lockopen + lockclose < open_today + close_today
                return (lockopen, lockclose, 1)
            else
                return (open_today, close_today, 0)
            end
        end
    elseif feetype == 1
        lockopen = feelist[2]*turnover*2
        lockclose = feelist[4]*turnover*2
        open_today = feelist[1]*turnover
        close_today = feelist[3]*turnover
        if postype == 1
            return (lockopen/2, lockclose/2, 0)
        else        
            if lockopen + lockclose < open_today + close_today
                return (lockopen, lockclose, 1)
            else
                return (open_today, close_today, 0)
            end
        end
    else
        return (0, 0, 0)
    end
end

function ordertrace_settle(sys_data::sysparam, date::Integer)
    closetime = Int32(153000000)
    opentime = Int32(203000000)
    ordertrace_last_orderitem = sys_data.ordertrace_last_orderitem
    ordertrace_settleprice = sys_data.ordertrace_settleprice
    symbols = [keys(ordertrace_last_orderitem)...]
    for symbol in symbols
        orderitem = copy(ordertrace_last_orderitem[symbol])
        if "trade_side" in keys(orderitem)
            product = orderitem["product"]
            if symbol in keys(ordertrace_settleprice)
                settleprice = ordertrace_settleprice[symbol]
                if orderitem["trade_side"] == "Long"
                    datetimeo = (date, closetime)
                    ordertrace_order_handle(sys_data, symbol, settleprice, "long_close", datetimeo, product, 1)
                    datetimeo = (date, opentime)
                    ordertrace_order_handle(sys_data, symbol, settleprice, "long_open", datetimeo, product, 1)
                elseif orderitem["trade_side"] == "Short"
                    datetimeo = (date, closetime)
                    ordertrace_order_handle(sys_data, symbol, settleprice, "short_close", datetimeo, product, 1)
                    datetimeo = (date, opentime)
                    ordertrace_order_handle(sys_data, symbol, settleprice, "short_open", datetimeo, product, 1)
                end
            else
                pop!(ordertrace_last_orderitem, symbol)
            end
        end
    end
end

function ordertrace_reset(sys_data::sysparam,date::Integer)
    ordertrace_settle(sys_data,date)
end
export ordertrace_reset

function td_order(sys_data::sysparam, symbol::String, side::String, nowdt::NTuple{2,Integer}; product::String="")::Int
#    id = Threads.threadid()
#    sys_data = enginedata[id]
    last_tick = sys_data.last_tick
    if !(symbol in keys(last_tick))
        return -5
    end
    if side in ["long_close","short_open"]
        price = last_tick[symbol][2]#bidprice
    elseif side in ["long_open","short_close"]
        price = last_tick[symbol][1]#askprice
    end    
    if price == 0
        return -6
    end
    if length(product) == 0
        product_default = getproduct(symbol)
        ordertrace_order_handle(sys_data, symbol, price, side, nowdt, product_default, 0)
    else
        ordertrace_order_handle(sys_data, symbol, price, side, nowdt, product, 0)
    end
end
export td_order


##################################################################
#risk
#################################################################
#######################################################################
##传入实时价格，计算最大回撤
#########################################################################
function risk_retracement(sys_data::sysparam, symbol::String, price::Number)
    ordertrace_last_orderitem = sys_data.ordertrace_last_orderitem
    ordertrace_last_netvalue = sys_data.ordertrace_last_netvalue
    ordertrace_max_netvalue = sys_data.ordertrace_max_netvalue 
    if symbol in keys(ordertrace_last_orderitem)
        orderitem = ordertrace_last_orderitem[symbol]
        if "price_open" in keys(orderitem)
            worstprice = orderitem["worstprice"]
            price_open = orderitem["price_open"]
            margin = orderitem["margin_ratio"]
            multiplier = orderitem["multiplier"]
            fee = orderitem["fee"]
            product = orderitem["product"]
            if product in keys(sys_data.ordertrace_retracement)
                last_retracement = sys_data.ordertrace_retracement[product]
            else
                last_retracement = 0
            end              
            if orderitem["trade_side"] == "Short"
                return_worst = (1 - worstprice/price_open)*10000/margin
                return_cur = (1-(price*multiplier+fee)/(price_open*multiplier))*10000/margin
            elseif orderitem["trade_side"] == "Long"
                return_worst = (worstprice/price_open - 1)*10000/margin
                return_cur = ((price*multiplier-fee)/(price_open*multiplier)-1)*10000/margin            
            end
            if product in keys(ordertrace_last_netvalue)
                max_value = ordertrace_max_netvalue[product]
                netvalue1 = ordertrace_last_netvalue[product] + return_worst
                netvalue2 = ordertrace_last_netvalue[product] + return_cur
            else
                max_value = 1
                netvalue1 = 1 + return_worst
                netvalue2 = 1 + return_cur
            end
            retracement1 = (netvalue1 - max(max_value, netvalue1))/max(max_value, netvalue1)
            retracement2 = (netvalue2 - max(max_value, netvalue1, netvalue2))/ max(max_value, netvalue1, netvalue2)
            last_retracement = min(retracement1,retracement2,last_retracement)
        end
    end
    return last_retracement
end
export risk_retracement

function risk_orderprice(sys_data::sysparam, symbol::String)
    ordertrace_last_orderitem = sys_data.ordertrace_last_orderitem
    if symbol in keys(ordertrace_last_orderitem)
        orderitem = ordertrace_last_orderitem[symbol]
        if "price_open" in keys(orderitem)
            side = orderitem["trade_side"]
            price = orderitem["price_open"]
            return (side, price)
        end
    end
    return ()
end
export risk_orderprice
#########################################################################################
#   save result
##########################################################################################
function strategy_filter(res::Vector{Tuple}, orderlist::Dict{String, Vector},
                        result::Vector{Tuple}, new_orderlist::Dict{String, Vector})
    ordertrace_generate_orderlist!(new_orderlist)
    target_products = Vector{String}()
    for restuple in res
        if (restuple[1] > 0) && (restuple[2] > -1)
            push!(result, restuple)
            push!(target_products, restuple[4])
        end
    end
    for i in 1:length(orderlist["product"])
        if orderlist["product"][i] in target_products
            for key in keys(new_orderlist)
                push!(new_orderlist[key], orderlist[key][i])
            end
        end
    end
end

global on_filter::Function = strategy_filter

function set_select_result_callback(custom_filter::Function)
    global on_filter = custom_filter
end
export set_select_result_callback

function write_csv(ordertrace_orderlist::Dict{String,Vector}, filename::String)
    io = open(filename, "w")
    head = "price_close,price_open,date_open,date_close,time_open,time_close,symbol,return_margin,margin_ratio,product,trade_side,return,price_tick,multiplier,basepoint,fee,islock,strategy_name,params,return_worst,return_worst_margin,return_best,return_best_margin,volume"
    colname = split(head, ",")
    write(io, string(head,"\n"))
    for i in 1:length(ordertrace_orderlist["price_close"])
        orderitem_str = ""
        for col in colname
            orderitem_str = string(orderitem_str, ordertrace_orderlist[col][i], ",") 
        end
        orderitem_str = string(orderitem_str[1:end-1], "\n")
        write(io, orderitem_str)
    end
    close(io)
end
export write_csv

function write_dolphindb(ordertrace_orderlist::Dict{String,Vector}, filename::String)
    ordertrace_orderlist["datetime_open"] = ordertrace_orderlist["date_open"]*1000000000+ordertrace_orderlist["time_open"]
    ordertrace_orderlist["datetime_close"] = ordertrace_orderlist["date_close"]*1000000000+ordertrace_orderlist["time_close"]   
    heads = String["price_close","price_open","datetime_open","datetime_close","symbol",
        "return_margin","margin_ratio","product","trade_side","return","price_tick",
        "multiplier","basepoint","fee","islock","strategy_name","params","return_worst",
        "return_worst_margin","return_best","return_best_margin","volume"]       
    for col_name in heads
        if col_name in ["datetime_open","datetime_close"]
            err1 = ddb_add_column(col_name, ordertrace_orderlist[col_name], "yyyyMMddHHmmssSSS")
        else
            err1 = ddb_add_column(col_name, ordertrace_orderlist[col_name])
        end
        if err1 < 0
            println(string(filename," errcode1:", err1))
            pop!(ordertrace_orderlist,"datetime_open")
            pop!(ordertrace_orderlist,"datetime_close")        
            write_csv(ordertrace_orderlist, filename)
            ddb_reset()
            return err1
        end        
    end
    err2 = ddb_upload_table(heads)
#    ddb_reset_table()    
    if err2 < 0
        println(string(filename," errcode3:", err2))
        pop!(ordertrace_orderlist,"datetime_open")
        pop!(ordertrace_orderlist,"datetime_close")        
        write_csv(ordertrace_orderlist, filename)
        ddb_reset()
    end
    return err2
end
export write_dolphindb

function write_job(orderlistch::Channel{Dict{String,Vector}})
    global logdir, savepath
    ordertrace_orderlist = take!(orderlistch)
    logfile = string(logdir, Dates.format(Dates.now(),"yyyymmdd_"),myid(),".log")
    io = open(logfile, "a")
    write(io, string(Dates.now()," : ", myid(), " data collection start\n"))
    i = 0
    while length(ordertrace_orderlist)>0
        i = i+1
        strategy_name = ordertrace_orderlist["strategy_name"][1]
        params = ordertrace_orderlist["params"][1]
        storepath = string(savepath,"/",strategy_name,"_",params,"/")
        mkpath(storepath)
        file_name = string(storepath, strategy_name, "_", params, "_",myid(),".csv")
        if save_mode == 4
            if i%5000 == 0
                dolphindb.ddb_reset()
            end
            err = write_dolphindb(ordertrace_orderlist, file_name)
            if err < 0 
                logline = string(file_name," errcode:", err, "\n")
                write(io, logline)
            end
        elseif save_mode == 3
            storepath = string(savepath,"/",strategy_name,"_",params,"/")
            mkpath(storepath)
            write_csv(ordertrace_orderlist, file_name)
        end
        ordertrace_orderlist = take!(orderlistch)
    end
    if save_mode == 4
        dolphindb.ddb_close_all()
    end
    write(io, string(Dates.now()," : ",myid()," data collection end. count:",i,"\n"))
    close(io)
end
export write_job
###############################################################################################
#### strategy 
###############################################################################################
function strategy_set_day_schedule_task(sys_data::sysparam, timepointer::Integer)
#    id = Threads.threadid()
#    sys_data = enginedata[id]
    if 0<= timepointer <= 240000
        if !(timepointer in sys_data.day_schedule_times)
            if sys_data.day_schedule_times[sys_data.timepoint_index] > timepointer
                sys_data.timepoint_index = sys_data.timepoint_index + 1
            end
            push!(sys_data.day_schedule_times, timepointer)
            sort!(sys_data.day_schedule_times)
        end
    end
    nothing
end
export strategy_set_day_schedule_task

function strategy_clear_day_schedule_task(sys_data::sysparam, timepoint::Integer)
#    id = Threads.threadid()
#    sys_data = enginedata[id]
    index = findfirst(isequal(timepoint), sys_data.day_schedule_times)
    while !(isnothing(index))
        if index < sys_data.timepoint_index
            sys_data.timepoint_index = sys_data.timepoint_index-1
        end
        popat!(sys_data.day_schedule_times, index)
        index = findfirst(isequal(timepoint), sys_data.day_schedule_times)    
    end
    sort!(sys_data.day_schedule_times)
    nothing
end
export strategy_clear_day_schedule_task

global on_day_schedule_task::Function
function strategy_set_day_schedule_task_callback(on_day::Function)
    global on_day_schedule_task = on_day
end
export strategy_set_day_schedule_task_callback

global on_futures_tick::Function
function md_set_futures_tick_callback(on_futures::Function)
    global on_futures_tick = on_futures
end
export md_set_futures_tick_callback

global on_init::Function
function strategy_set_init_callback(on_init_func::Function)
    global on_init = on_init_func
end
export strategy_set_init_callback

global on_exit::Function
function strategy_set_exit_callback(on_exit_func::Function)
    global on_exit = on_exit_func
end
export strategy_set_exit_callback

function strategy_get_margin_ratio(sys_data::sysparam)
#    id = Threads.threadid()
#    sys_data = enginedata[id]
    return sys_data.ordertrace_margin_ratio
end
export strategy_get_margin_ratio

function strategy_get_price_tick(sys_data::sysparam)
#    id = Threads.threadid()
#    sys_data = enginedata[id]
    return sys_data.ordertrace_price_tick
end
export strategy_get_price_tick

function strategy_get_major_codes(sys_data::sysparam)
#    id = Threads.threadid()
#    sys_data = enginedata[id]
    return sys_data.ordertrace_major_codes
end
export strategy_get_major_codes

function strategy_get_tradedate(sys_data::sysparam)
#    id = Threads.threadid()
#    sys_data = enginedata[id]
    return sys_data.ordertrace_tradeday
end
export strategy_get_tradedate

function run_with_params(strategy_name::String, params::String, external_data)
#    global enginedata, 
    global on_day_schedule_task, on_futures_tick,on_select_result, savepath, save_mode
    global on_init, on_exit
#    id = Threads.threadid()
#    sys_data = enginedata[id]
#    reset_sysdata!(sys_data)
#    sys_data.strategy_name = strategy_name
#    sys_data.params = params
    sys_data = external_data.sys_data
    ordertrace_load!(sys_data)
    on_init(external_data)
    flag = true
    cur_date = 0
    sys_data.timepoint_index = 1
    for i in eachindex(dates)
        symbols = symbols2d[i]
        datetimes = datetimes2d[i]
        futureticks = custom2d[i]
        margin_ratio = margin_ratio2d[i]
        price_tick = price_tick2d[i]
        multiplier = multiplier2d[i]
        lftmatchs = lftmatch2d[i]
        settleprice = settleprice2d[i]
        major_codes = major_codes2d[i]
        if length(symbols) == 0
            continue
        end
        date = parse(Int, Dates.format(dates[i],"yyyymmdd"))
        ordertrace_init!(sys_data, margin_ratio, price_tick, multiplier, settleprice, major_codes, date)
        for j in eachindex(symbols)
            symbol = symbols[j]
            nowdt = datetimes[j]
            tickdata = futureticks[j]
            lftmatchdata = lftmatchs[j]
            if flag
                cur_date = nowdt[1]
                flag = false
            end            
            cur_time = sys_data.day_schedule_times[sys_data.timepoint_index]
            if cur_date == nowdt[1]
                while nowdt[2] >= cur_time*1000
                    sys_data.timepoint_index = sys_data.timepoint_index + 1
                    on_day_schedule_task(cur_time, external_data)
                    cur_time = sys_data.day_schedule_times[sys_data.timepoint_index]
                end
            else
                endtime = 240000
                while endtime >= cur_time
                    sys_data.timepoint_index = sys_data.timepoint_index + 1
                    on_day_schedule_task(cur_time, external_data)
                    cur_time = sys_data.day_schedule_times[sys_data.timepoint_index]
                end
                cur_date = nowdt[1]
                sys_data.timepoint_index = 1
            end
            sys_data.last_tick[symbol] = (lftmatchdata.ask_price, lftmatchdata.bid_price)
            on_futures_tick(date, symbol, nowdt, tickdata, external_data)
            ordertrace_setworstprice!(sys_data, symbol, lftmatchdata.high, lftmatchdata.low)
        end       
        ordertrace_reset(sys_data, date)
    end
    strategy_summary = ordertrace_profit_return(sys_data)
    new_summary = Vector{Tuple}()
    new_orderlist = Dict{String, Vector}()
    on_filter(strategy_summary, sys_data.ordertrace_orderlist, new_summary, new_orderlist)
    on_exit(new_orderlist, external_data)
    len = length(new_orderlist["price_close"])
    if len != 0  
        new_orderlist["strategy_name"] = [strategy_name for i in 1:len]
        new_orderlist["params"] = [params for i in 1:len]
        storepath = string(savepath,"/",strategy_name,"_",params,"/")
        filename = string(savepath,"/",strategy_name,"_",params,"/",strategy_name,
        "_",params,"_",myid(),".csv")
        if save_mode in [3,4]
            push!(orderlistch, new_orderlist)
        elseif save_mode == 1
            mkpath(storepath)
            write_csv(new_orderlist, filename)
        elseif save_mode == 2
            write_dolphindb(new_orderlist, filename)
        end
    end
    return new_summary
end
export run_with_params

function strategy_init(paramdict::Dict{String,Any})
    global holidayfile, orderfee_file, begin_date, end_date, folder, logdir, save_mode, savepath
    global host, port, usr, psw, table_name, db_path, dolphindb_dll, orderlistch
    if "holidayfile" in keys(paramdict)
        holidayfile = paramdict["holidayfile"]
    end
    if "orderfee" in keys(paramdict)
        orderfee_file = paramdict["orderfee"]
    end
    if issubset(["begin","end"],  keys(paramdict))
        setdates(paramdict["begin"], paramdict["end"])
    end
    if "md_path" in keys(paramdict)
        folder = paramdict["md_path"]
    end
    if "log_path" in keys(paramdict)
        logdir = paramdict["log_path"]
    end
    if "save_mode" in keys(paramdict)
        save_mode = paramdict["save_mode"]
    end
    if "save_path" in keys(paramdict)
        savepath = paramdict["save_path"]
    end
    if issubset(["host","port","usr","psw","table_name"], keys(paramdict))
        host = paramdict["host"]
        port = paramdict["port"]
        usr = paramdict["usr"]
        psw = paramdict["psw"]
        table_name = paramdict["table_name"]
        if "db_path" in keys(paramdict)
            db_path = paramdict["db_path"]
            dolphindb.init(host, port, usr, psw, table_name, db_path)
        else
            dolphindb.init(host, port, usr, psw, table_name)
        end
    end
    if save_mode in [3,4]
        global wtask = @async write_job(orderlistch)
    end
#    global enginedata = Vector{sysparam}()
#    n = Threads.nthreads()
#    for i in 1:n
#        push!(enginedata, sysparam("", ""))
#    end
    nothing
end
export strategy_init

function strategy_close_write_job()
    global orderlistch, save_mode
    if save_mode in [3,4]
        orderlist_empty = Dict{String,Vector}()
        push!(orderlistch, orderlist_empty)
    else
        dolphindb.ddb_close_all()
    end
    nothing
end
export strategy_close_write_job

end
