module qengine
using HDB
using Dates
using CBinding
using StringEncodings
using Statistics
using Distributed
using dolphindb
import FinancialStruct
using FinancialStruct:cFuturesTickData,cSecurityTickData
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
            # 使用 BarBuilder 代替原有的 high/low 维护逻辑
            bb = BarBuilder(generate, param_type())
            
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
            for j in eachindex(futureticks)
                tick = futureticks[j]
                symbol = psymbols[j]
                nowdt = parse_datetime(futureitems[j])
                
                # 保存结算价
                if tick.settle_price > 0
                    settleprice[symbol] = tick.settle_price
                end
                
                # 使用 BarBuilder.feed_tick!（内置时间过滤）
                out = feed_tick!(bb, datei, symbol, nowdt, tick)
                if out !== nothing
                    bar, match = out
                    push!(lftmatchvec, match)
                    push!(lftdatavec, bar)
                    push!(symbols, symbol)
                    push!(datetimes, nowdt)
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
            # 使用 BarBuilder 代替原有的 high/low 维护逻辑
            bb = BarBuilder(generate, param_type())
            
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
            for j in eachindex(futureticks)
                tick = futureticks[j]
                symbol = psymbols[j]
                nowdt = parse_datetime(futureitems[j])
                
                # 保存结算价
                if tick.settle_price > 0
                    settleprice[symbol] = tick.settle_price
                end
                
                # 使用 BarBuilder.feed_tick!（内置时间过滤）
                out = feed_tick!(bb, datei, symbol, nowdt, tick)
                if out !== nothing
                    bar, match = out
                    push!(lftmatchvec, match)
                    push!(lftdatavec, bar)
                    push!(symbols, symbol)
                    push!(datetimes, nowdt)
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
            # 使用 BarBuilder 代替原有的 high/low 维护逻辑
            bb = BarBuilder(generate, param_type())
            
            load_securityticks!(file_id, futureitems, securityticks, part_symbols)
            psymbols = parse_symbols(futureitems)
            for j in eachindex(securityticks)
                tick = securityticks[j]
                symbol = psymbols[j]
                nowdt = parse_datetime(futureitems[j])
                if tick.match > 0
                    settleprice[symbol] = tick.match
                end
                # 使用 BarBuilder.feed_tick! 代替原有逻辑
                # 注意：security 类型的 generate 函数不需要 tradeday 参数
                # 使用 datei 作为 tradeday 参数传入
                out = feed_tick!(bb, datei, symbol, nowdt, tick)
                if out !== nothing
                    bar, match = out
                    push!(lftmatchvec, match)
                    push!(lftdatavec, bar)
                    push!(symbols, symbol)
                    push!(datetimes, nowdt)
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

#═══════════════════════════════════════════════════════════
# BarBuilder：流式 bar 构建器
#═══════════════════════════════════════════════════════════

"""
流式 bar 构建器，支持逐 tick 喂入，自动聚合生成低频数据

类型参数：
- P: 生成函数所需的状态类型（如 dataparam）

示例：
    bb = BarBuilder(generatekline_night, dataparam())
    for tick in tick_stream
        out = feed_tick!(bb, tradeday, symbol, nowdt, tick)
        if out !== nothing
            bar, match = out
            # 处理生成的 bar
        end
    end
"""
mutable struct BarBuilder{P}
    state::P                          # 生成函数所需状态（如 dataparam）
    high::Dict{String,Int64}          # 当前 bar 内每个 symbol 的最高价
    low::Dict{String,Int64}           # 当前 bar 内每个 symbol 的最低价
    generate::Function                # (tradeday, symbol, nowdt, tick, state) -> Union{bar,Nothing}
end

"""
构造函数

参数：
- generate: K线生成函数（generatekline_night / generateTline_night / 自定义）
- state_instance: 生成函数所需状态实例（如 dataparam()）

示例：
    bb = BarBuilder(generatekline_night, dataparam())
"""
function BarBuilder(generate::Function, state_instance::P) where {P}
    BarBuilder{P}(
        state_instance,
        Dict{String,Int64}(),
        Dict{String,Int64}(),
        generate
    )
end

export BarBuilder

"""
feed_tick!(bb::BarBuilder, tradeday::Int, symbol::String, nowdt::NTuple{2,Int}, tick)

逐 tick 喂入数据，自动维护 high/low 并调用生成函数

根据 tick 类型自动派发：
- FuturesTick: 调用 generate(tradeday, symbol, nowdt, tick, state)
- SecurityTick: 调用 generate(symbol, nowdt, tick, state)
- 未来可扩展更多类型

内置功能：
1. **时间过滤**：自动过滤时间偏差过大的 tick
   - 期货：夜盘允许最大偏差 3600秒，日盘允许最大偏差 180秒
   - 证券：只有日盘，允许最大偏差 180秒
2. **high/low 维护**：自动跟踪当前 bar 内的最高/最低价
3. **bar 生成**：调用用户提供的生成函数
4. **撮合信息**：自动构造 lftmatch 结构

参数：
- bb: BarBuilder 实例
- tradeday: 交易日（yyyymmdd 格式）
- symbol: 合约代码
- nowdt: (date_int, time_int) 时间戳
- tick: FuturesTick 或 SecurityTick

返回：
- nothing: 当前 tick 被过滤或未触发 bar 结束
- (bar, lftmatch): 生成的 bar 和撮合信息

示例：
    # 期货
    out = feed_tick!(bb, 20250101, "SHFE.rb2505", (20250101, 93015000), futures_tick)
    if out !== nothing
        bar, match = out
        # 处理生成的 bar
    end
    
    # 证券
    out = feed_tick!(bb, 20250101, "SH.600000", (20250101, 93015000), security_tick)
"""
# 期货版本：调用5参数生成函数
function feed_tick!(bb::BarBuilder, tradeday::Int, symbol::String, 
                    nowdt::NTuple{2,Integer}, tick::FuturesTick)
    # 1) 时间过滤：检查 tick 时间与系统时间的偏差
    dt = CTime(nowdt[2], tick.time)
    if 210000000 > nowdt[2] > 150000000
        # 夜盘时间段：允许最大偏差 3600 秒
        if abs(dt) > 3600
            return nothing
        end
    else
        # 日盘时间段：允许最大偏差 180 秒
        if abs(dt) > 60*3
            return nothing
        end
    end
    
    # 2) 更新 high/low（仅当 tick.match != 0 时）
    if tick.match != 0
        if symbol in keys(bb.high)
            bb.high[symbol] = max(tick.match, bb.high[symbol])
            bb.low[symbol] = min(tick.match, bb.low[symbol])
        else
            bb.high[symbol] = tick.match
            bb.low[symbol] = tick.match
        end
    end
    
    # 3) 调用期货生成函数（5参数）
    bar = bb.generate(tradeday, symbol, nowdt, tick, bb.state)
    
    if isnothing(bar)
        return nothing
    end
    
    # 4) 构造 lftmatch
    match = lftmatch(
        bb.high[symbol],
        bb.low[symbol],
        tick.ask_price[1],
        tick.ask_vol[1],
        tick.bid_price[1],
        tick.bid_vol[1]
    )
    
    # 5) 清除 high/low 状态
    pop!(bb.high, symbol)
    pop!(bb.low, symbol)
    
    return (bar, match)
end

# 证券版本：调用4参数生成函数
function feed_tick!(bb::BarBuilder, tradeday::Int, symbol::String, 
                    nowdt::NTuple{2,Integer}, tick::SecurityTick)
    # 1) 时间过滤：检查 tick 时间与系统时间的偏差
    # 证券交易只有日盘，允许最大偏差 180 秒
    dt = CTime(nowdt[2], tick.time)
    if abs(dt) > 60*3
        return nothing
    end
    
    # 2) 更新 high/low（仅当 tick.match != 0 时）
    if tick.match != 0
        if symbol in keys(bb.high)
            bb.high[symbol] = max(tick.match, bb.high[symbol])
            bb.low[symbol] = min(tick.match, bb.low[symbol])
        else
            bb.high[symbol] = tick.match
            bb.low[symbol] = tick.match
        end
    end
    
    # 3) 调用证券生成函数（4参数）
    bar = bb.generate(symbol, nowdt, tick, bb.state)
    
    if isnothing(bar)
        return nothing
    end
    
    # 4) 构造 lftmatch
    match = lftmatch(
        bb.high[symbol],
        bb.low[symbol],
        tick.ask_price[1],
        tick.ask_vol[1],
        tick.bid_price[1],
        tick.bid_vol[1]
    )
    
    # 5) 清除 high/low 状态
    pop!(bb.high, symbol)
    pop!(bb.low, symbol)
    
    return (bar, match)
end

# 未来可轻松扩展其他类型，例如加密货币：
# function feed_tick!(bb::BarBuilder, tradeday::Int, symbol::String, 
#                     nowdt::NTuple{2,Int}, tick::CryptoTick)
#     if tick.match != 0
#         if symbol in keys(bb.high)
#             bb.high[symbol] = max(tick.match, bb.high[symbol])
#             bb.low[symbol] = min(tick.match, bb.low[symbol])
#         else
#             bb.high[symbol] = tick.match
#             bb.low[symbol] = tick.match
#         end
#     end
#     
#     # 调用加密货币生成函数（6参数，可能需要额外参数）
#     exchange = get_exchange(symbol)
#     fee_rate = get_fee_rate(exchange)
#     bar = bb.generate(exchange, symbol, nowdt, tick, fee_rate, bb.state)
#     
#     if bar === nothing
#         return nothing
#     end
#     
#     match = lftmatch(
#         bb.high[symbol], bb.low[symbol],
#         tick.ask_price[1], tick.ask_vol[1],
#         tick.bid_price[1], tick.bid_vol[1]
#     )
#     
#     pop!(bb.high, symbol)
#     pop!(bb.low, symbol)
#     
#     return (bar, match)
# end

export feed_tick!

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
    ordertrace_margin_ratio, ordertrace_price_tick, ordertrace_multiplier, ordertrace_fee, 
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

#═══════════════════════════════════════════════════════════
# 在线模拟定时器状态
#═══════════════════════════════════════════════════════════

# OnlineTimerState 已移除
# 理由：
# 1. current_tradeday 可直接使用 sys_data.ordertrace_tradeday 替代
# 2. 真实时间严格递增，无需 last_triggered_time 去重
# 3. 减少冗余状态，简化设计

#═══════════════════════════════════════════════════════════
# 事件驱动接口：新交易日初始化
#═══════════════════════════════════════════════════════════

"""
strategy_on_new_day!(sys_data, date, margin_ratio, price_tick, multiplier, settleprice, major_codes)

新交易日开始：初始化合约参数、重置日内调度（离线回测版本）

用于离线回测，从 *2d 数组传入参数。

参数：
- sys_data: 策略状态
- date: 交易日
- margin_ratio: 保证金率字典（从 margin_ratio2d[i] 获取）
- price_tick: 最小变动价位字典（从 price_tick2d[i] 获取）
- multiplier: 合约乘数字典（从 multiplier2d[i] 获取）
- settleprice: 结算价字典（从 settleprice2d[i] 获取）
- major_codes: 主力合约列表（从 major_codes2d[i] 获取）

示例（在 run_with_params 中调用）：
    strategy_on_new_day!(sys_data, date, margin_ratio2d[i], 
                        price_tick2d[i], multiplier2d[i], 
                        settleprice2d[i], major_codes2d[i])
"""
function strategy_on_new_day!(
    sys_data::sysparam,
    date::Integer,
    margin_ratio::Dict{String,NTuple{2,Integer}},
    price_tick::Dict{String,Integer},
    multiplier::Dict{String,Integer},
    settleprice::Dict{String,Integer},
    major_codes::Vector{String}
)
    ordertrace_init!(sys_data, margin_ratio, price_tick, multiplier, settleprice, major_codes, date)
    return nothing
end

export strategy_on_new_day!

#═══════════════════════════════════════════════════════════
# 事件驱动接口：交易日结束
#═══════════════════════════════════════════════════════════

"""
strategy_on_day_end!(sys_data, date)

交易日结束：执行结算、清理日内状态

会调用 ordertrace_reset 进行日终结算。

参数：
- sys_data: 策略状态
- date: 交易日

示例：
    strategy_on_day_end!(sys_data, 20250101)
"""
function strategy_on_day_end!(sys_data::sysparam, date::Int)
    ordertrace_reset(sys_data, date)
    return nothing
end

export strategy_on_day_end!

#═══════════════════════════════════════════════════════════
# 在线模拟数据结构
#═══════════════════════════════════════════════════════════

# 全局 BarBuilder 实例（所有策略实例共享）
const GLOBAL_BAR_BUILDER = Ref{Union{BarBuilder,Nothing}}(nothing)

#═══════════════════════════════════════════════════════════
# 在线模拟初始化函数
#═══════════════════════════════════════════════════════════

"""init_online_simulation(; linetype="minkline", instrument="future")

初始化在线模拟环境（创建全局共享的 BarBuilder）

参数：
- linetype: K线类型，可选 "minkline"（默认，1分钟K线）或 "tline"（T线）
- instrument: 标的类型，默认"future"（期货），可选"future"、"security"（证券）

返回：
- bb: BarBuilder 实例（全局共享单例）

注意：
- 用户需要自己创建 external_data（包含 sys_data 和策略特定字段）
- external_data 必须包含 sys_data 字段
- sys_data 通过 sysparam(strategy_name, params) 创建
- 用户需要在 external_data 上实现 4 个方法（duck typing）：
  - get_margin_ratio(external_data, tradeday)
  - get_price_tick(external_data, tradeday)
  - get_multiplier(external_data, tradeday)
  - get_major_codes(external_data, tradeday)

使用示例（参考 docs/strategy10.jl）：
    # 1. 初始化全局 BarBuilder
    bb = init_online_simulation(linetype="minkline", instrument="future")
    
    # 2. 用户创建自己的 external_data（包含策略特定字段）
    external_data = strategy_param(
        "EMA_FUTURE",           # strategy_name
        "20_10_5",              # params
        ["SHFE.au"],            # products
        20,                      # long
        10,                      # short
        5                        # period
    )
    # strategy_param 内部会创建 sys_data = sysparam(strategy_name, params)
    
    # 3. 在主循环中使用
    while running
        on_time_heartbeat(tradeday, current_time, external_data)
        sleep(1)
    end
    
    for tick in market_stream
        on_md_tick(tradeday, symbol, nowdt, tick, bb, external_data)
    end

多实例场景：
    # 1. 初始化全局资源
    bb = init_online_simulation()
    
    # 2. 创建多个策略实例
    instance1 = strategy_param("EMA_FUTURE", "20_10_5", ["SHFE.au"], 20, 10, 5)
    instance2 = strategy_param("EMA_FUTURE", "30_15_3", ["SHFE.ag"], 30, 15, 3)
    
    # 3. 使用（共享 bb，独立 external_data）
    for tick in market_stream
        on_md_tick(tradeday, symbol, nowdt, tick, instance1)
        on_md_tick(tradeday, symbol, nowdt, tick, instance2)
    end
"""
function init_online_simulation(;
    linetype::String="minkline",
    instrument::String="future"
)
    # 1. 获取或创建全局 BarBuilder（所有实例共享）
    if GLOBAL_BAR_BUILDER[] === nothing
        if instrument == "future"
            if linetype == "minkline"
                GLOBAL_BAR_BUILDER[] = BarBuilder(generatekline_night, dataparam())
            elseif linetype == "tline"
                GLOBAL_BAR_BUILDER[] = BarBuilder(generateTline_night, dataparam())
            else
                error("期货不支持的线型: $linetype, 可选: minkline, tline")
            end
        elseif instrument == "security"
            if linetype == "minkline"
                GLOBAL_BAR_BUILDER[] = BarBuilder(generatekline, dataparam())
            else
                error("证券目前仅支持 linetype=\"minkline\"")
            end
        else
            error("不支持的标的类型: $instrument, 可选: future, security")
        end
        @info "创建全局 BarBuilder (linetype=$linetype, instrument=$instrument)，所有策略实例将共享此实例"
    end
    bb = GLOBAL_BAR_BUILDER[]
    
    return bb
end

"""
reset_global_barbuilder!()

重置全局 BarBuilder 实例（用于切换 linetype 或重新开始模拟）

使用场景：
- 需要切换 K线类型（minkline ↔ tline）
- 需要切换标的类型（future ↔ security）
- 重新开始新的模拟会话

示例：
    # 第一次使用 minkline
    bb = init_online_simulation(linetype="minkline")
    
    # 切换到 tline 前先重置
    reset_global_barbuilder!()
    bb = init_online_simulation(linetype="tline")
    
    # 从期货切换到证券
    reset_global_barbuilder!()
    bb = init_online_simulation(instrument="security")
"""
function reset_global_barbuilder!()
    GLOBAL_BAR_BUILDER[] = nothing
    @info "已重置全局 BarBuilder"
end

export init_online_simulation, reset_global_barbuilder!

#═══════════════════════════════════════════════════════════
# 在线模拟示例代码（用户项目中实现）
#═══════════════════════════════════════════════════════════

"""
在线模拟时间心跳回调

功能：
- 跨日检测（唯一权威的时间源）
- 执行日终结算（strategy_on_day_end!）
- 执行新日初始化（strategy_on_new_day!）
- 触发定时任务（on_day_schedule_task）

参数：
- tradeday: 当前交易日（yyyymmdd）
- current_time: 当前时间（HHMMSS格式，如 143000 表示 14:30:00）
- external_data: 外部数据（用户自定义类型）
- margin_ratio: (可选) 保证金率字典，如果不传则调用 get_margin_ratio 方法获取
- price_tick: (可选) 最小变动价位字典，如果不传则调用 get_price_tick 方法获取  
- multiplier: (可选) 合约乘数字典，如果不传则调用 get_multiplier 方法获取
- major_codes: (可选) 主力合约列表，如果不传则调用 get_major_codes 方法获取

使用方式1：传入参数（推荐，适合已有参数的场景）
    # 用户在开盘前获取参数（一天一次）
    daily_margin_ratio = fetch_margin_ratio_from_db(tradeday)
    daily_price_tick = fetch_price_tick_from_db(tradeday)
    daily_multiplier = fetch_multiplier_from_db(tradeday)
    daily_major_codes = fetch_major_codes_from_db(tradeday)
    
    # 主循环中直接传入
    while running
        on_time_heartbeat(
            get_current_tradeday(),
            get_current_hhmmss(),
            my_data,
            margin_ratio = daily_margin_ratio,
            price_tick = daily_price_tick,
            multiplier = daily_multiplier,
            major_codes = daily_major_codes
        )
        sleep(1)
    end

使用方式2：不传参数（向后兼容，需实现4个方法）
    # 用户实现4个方法
    function get_margin_ratio(sp::MyStrategy, tradeday::Int)
        return Dict("SHFE.au" => (12, 12))
    end
    # ... 其他3个方法
    
    # 主循环中调用（自动调用方法获取参数）
    while running
        on_time_heartbeat(
            get_current_tradeday(),
            get_current_hhmmss(),
            my_data  # qengine会自动调用4个方法
        )
        sleep(1)
    end

注意：
- settleprice 始终从 tick 数据自动提取，无需传入
- 这4个参数一天只在开盘前更新一次，盘中不变
- 所有实例共享同一套参数
"""
# on_time_heartbeat 由 qengine 实现并导出，用户直接调用即可

function on_time_heartbeat(
    tradeday::Integer,
    current_time::Integer,
    external_data;
    margin_ratio::Union{Dict{String,NTuple{2,Integer}},Nothing} = nothing,
    price_tick::Union{Dict{String,Integer},Nothing} = nothing,
    multiplier::Union{Dict{String,Integer},Nothing} = nothing,
    major_codes::Union{Vector{String},Nothing} = nothing
)
    sys_data = external_data.sys_data

    # 1) 跨日检测：使用 sys_data.ordertrace_tradeday 作为权威
    if tradeday != sys_data.ordertrace_tradeday
        # 1.1 上一交易日日终结算
        if sys_data.ordertrace_tradeday != 0
            strategy_on_day_end!(sys_data, sys_data.ordertrace_tradeday)
        end

        # 1.2 获取当日合约参数（优先使用传入参数，否则使用前一交易日的值）
        _margin_ratio = isnothing(margin_ratio) ? sys_data.ordertrace_margin_ratio : copy(margin_ratio)
        _price_tick   = isnothing(price_tick)   ? sys_data.ordertrace_price_tick   : copy(price_tick)
        _multiplier   = isnothing(multiplier)   ? sys_data.ordertrace_multiplier   : copy(multiplier)
        _major_codes  = isnothing(major_codes)  ? sys_data.ordertrace_major_codes  : copy(major_codes)

        # 在线模式下，结算价由 tick 数据逐步填充，这里传入空字典
        settleprice = Dict{String, Integer}()

        # 1.3 新交易日初始化（内部会设置 sys_data.ordertrace_tradeday = tradeday）
        strategy_on_new_day!(
            sys_data,
            tradeday,
            _margin_ratio,
            _price_tick,
            _multiplier,
            settleprice,
            _major_codes,
        )
    end

    # 2) 定时任务触发：直接判断 current_time 是否在调度列表中
    # 注意：真实时间严格递增，无需去重检查
    if current_time in sys_data.day_schedule_times
        global on_day_schedule_task
        on_day_schedule_task(current_time, external_data)
    end

    return nothing
end

export on_time_heartbeat

"""on_time_heartbeat_multi(tradeday, current_time, external_datas; kwargs...)

时间心跳处理（多实例版本）

集中管理所有实例的时间相关状态：跨日、日初/日终、定时任务

参数:
- tradeday: 当前交易日（yyyymmdd）
- current_time: 当前时间（HHMMSS格式，如143000表示14:30:00）
- external_datas: Vector，用户创建的 external_data 实例列表
- margin_ratio: (可选) 保证金率字典，所有实例共享
- price_tick: (可选) 最小变动价位字典，所有实例共享
- multiplier: (可选) 合约乘数字典，所有实例共享
- major_codes: (可选) 主力合约列表，所有实例共享

逻辑流程：
为每个实例调用 on_time_heartbeat，每个实例独立处理：
1. 跨日检测
   - strategy_on_day_end!（日终）
   - 获取参数（优先使用传入参数，否则调用方法）
   - strategy_on_new_day!（日初）
2. 定时任务触发
   - on_day_schedule_task

用户示例（推荐）:
    # 创建实例
    instances = [
        strategy_param("EMA_FUTURE", "20_10_5", ["SHFE.au"], 20, 10, 5),
        strategy_param("MA_FUTURE", "30_15_3", ["SHFE.ag"], 30, 15, 3)
    ]
    
    # 开盘前获取参数（一天一次）
    daily_params = fetch_daily_params(tradeday)
    
    # 时间心跳任务
    timer_task = @async begin
        while running
            on_time_heartbeat_multi(
                get_current_tradeday(),
                get_current_hhmmss(),
                instances,
                margin_ratio = daily_params.margin_ratio,
                price_tick = daily_params.price_tick,
                multiplier = daily_params.multiplier,
                major_codes = daily_params.major_codes
            )
            sleep(1)
        end
    end
"""
function on_time_heartbeat_multi(
    tradeday::Integer,
    current_time::Integer,
    external_datas::Vector;
    margin_ratio::Union{Dict{String,NTuple{2,Integer}},Nothing} = nothing,
    price_tick::Union{Dict{String,Integer},Nothing} = nothing,
    multiplier::Union{Dict{String,Integer},Nothing} = nothing,
    major_codes::Union{Vector{String},Nothing} = nothing
)
    # 为每个实例调用单实例版本的 on_time_heartbeat
    # 注意：所有实例共享同一套参数
    if length(external_datas) == 0
        return  # 无实例，直接返回
    end
    
    for instance in external_datas
        on_time_heartbeat(
            tradeday,
            current_time,
            instance,
            margin_ratio = margin_ratio,
            price_tick = price_tick,
            multiplier = multiplier,
            major_codes = major_codes
        )
    end
    
    return nothing
end

export on_time_heartbeat_multi

"""
在线模拟行情回调（使用示例）

注意：这是示例代码，用户需要在自己的项目中实现。
详细实现请参考 docs/REFACTOR_COMPLETE_PLAN.md 中的完整示例。

功能：
- tick → bar 转换
- 策略回调（on_futures_tick）
- 风控更新

参数：
- tradeday: 交易日
- symbol: 合约代码
- nowdt: (date_int, time_int) 当前时间
- raw_tick: FuturesTick 或 SecurityTick
- external_data: 外部数据（包含 sys_data 等）

示例用法：
    # 初始化全局 BarBuilder
    init_online_simulation()
    
    # 在外部tick回调中调用（无需传入 bb）
    function on_market_data(tradeday, symbol, nowdt, tick)
        on_md_tick(tradeday, symbol, nowdt, tick, external_data)
    end
"""
# on_md_tick 由用户实现，请参考 docs/REFACTOR_COMPLETE_PLAN.md
# 注意：从 v3.0 起，qengine 同时提供了内置的 on_md_tick 实现，用户可直接调用。

function on_md_tick(
    tradeday::Int,
    nowdt::NTuple{2,Int},
    ftick::cFuturesTickData,
    external_data,
)
    raw_tick = FuturesTick(
        time = ftick.time,
        status = ftick.status,
        pre_open_interest = ftick.pre_open_interest,
        pre_close = ftick.pre_close,
        pre_settle_price = ftick.pre_settle_price,
        open = ftick.open,
        high = ftick.high,
        low = ftick.low,
        match = ftick.match,
        volume = ftick.volume,
        turnover = ftick.turnover,
        open_interest = ftick.open_interest,
        close = ftick.close,
        settle_price = ftick.settle_price,
        high_limited = ftick.high_limited,
        low_limited = ftick.low_limited,
        pre_delta = ftick.pre_delta,
        curr_delta = ftick.curr_delta,
        ask_price = ftick.ask_price,  # 确保类型匹配（如 Carray{Int64,5}）
        ask_vol = ftick.ask_vol,
        bid_price = ftick.bid_price,
        bid_vol = ftick.bid_vol,
        trading_status = UInt8(ftick.trading_status)  # 显式类型转换
    )
    symbol = unsafe_string(convert(Ptr{UInt8}, ftick.symbol))

    # 适配到 FuturesTick 版本，复用核心逻辑
    on_md_tick(tradeday, symbol, nowdt, raw_tick, external_data)

    return nothing
end

function on_md_tick(
    tradeday::Int,
    symbol::String,
    nowdt::NTuple{2,Int},
    raw_tick::FuturesTick,
    external_data,
)
    sys_data = external_data.sys_data

    # 1) 更新结算价（在线模式下从 tick.settle_price 提取）
    # 注意：无论是否生成bar，只要tick中有结算价就立即更新
    if raw_tick.settle_price > 0
        sys_data.ordertrace_settleprice[symbol] = raw_tick.settle_price
    end

    # 2) 从全局获取 BarBuilder
    bb = GLOBAL_BAR_BUILDER[]
    if bb === nothing
        @error "BarBuilder 未初始化，请先调用 init_online_simulation"
        return nothing
    end

    # 3) tick → (bar, match)
    out = feed_tick!(bb, tradeday, symbol, nowdt, raw_tick)
    if out === nothing
        return nothing
    end
    bar, match = out

    # 4) 更新 last_tick（用于 td_order）并调用策略回调
    sys_data.last_tick[symbol] = (match.ask_price, match.bid_price)

    global on_futures_tick
    on_futures_tick(tradeday, symbol, nowdt, bar, external_data)

    # 5) 更新风控最差价
    ordertrace_setworstprice!(sys_data, symbol, match.high, match.low)

    return nothing
end

function on_md_tick(
    tradeday::Int,
    nowdt::NTuple{2,Int},
    stick::cSecurityTickData,
    external_data,
)
    raw_tick = SecurityTick(
        time                  = stick.time,
        status                = stick.status,
        pre_close             = stick.pre_close,
        open                  = stick.open,
        high                  = stick.high,
        low                   = stick.low,
        match                 = stick.match,
        ask_price             = stick.ask_price,
        ask_vol               = stick.ask_vol,
        bid_price             = stick.bid_price,
        bid_vol               = stick.bid_vol,
        num_trades            = stick.num_trades,
        volume                = stick.volume,
        turnover              = stick.turnover,
        total_bid_vol         = stick.total_bid_vol,
        total_ask_vol         = stick.total_ask_vol,
        weighted_avg_bid_price = stick.weighted_avg_bid_price,
        weighted_avg_ask_price = stick.weighted_avg_ask_price,
        iopv                  = stick.iopv,
        yield_to_maturity     = stick.yield_to_maturity,
        high_limited          = stick.high_limited,
        low_limited           = stick.low_limited,
        prefix                = stick.prefix,
        syl1                  = stick.syl1,
        syl2                  = stick.syl2,
        sd2                   = stick.sd2,
        trading_phase_code    = stick.trading_phase_code,
        pre_iopv              = stick.pre_iopv,
    )
    symbol = unsafe_string(convert(Ptr{UInt8}, stick.symbol))

    # 适配到 SecurityTick 核心版本，复用逻辑
    on_md_tick(tradeday, symbol, nowdt, raw_tick, external_data)

    return nothing
end

function on_md_tick(
    tradeday::Int,
    symbol::String,
    nowdt::NTuple{2,Int},
    raw_tick::SecurityTick,
    external_data,
)
    sys_data = external_data.sys_data

    # 1) 证券场景：使用成交价作为结算价
    # 注意：无论是否生成bar，只要tick中有成交价就立即更新
    if raw_tick.match > 0
        sys_data.ordertrace_settleprice[symbol] = raw_tick.match
    end

    # 2) 从全局获取 BarBuilder
    bb = GLOBAL_BAR_BUILDER[]
    if bb === nothing
        @error "BarBuilder 未初始化，请先调用 init_online_simulation"
        return nothing
    end

    # 3) tick → (bar, match)
    out = feed_tick!(bb, tradeday, symbol, nowdt, raw_tick)
    if out === nothing
        return nothing
    end
    bar, match = out

    # 4) 更新 last_tick 并调用策略回调
    sys_data.last_tick[symbol] = (match.ask_price, match.bid_price)

    global on_futures_tick
    on_futures_tick(tradeday, symbol, nowdt, bar, external_data)

    # 5) 更新风控最差价
    ordertrace_setworstprice!(sys_data, symbol, match.high, match.low)

    return nothing
end

export on_md_tick

"""on_md_tick_multi(tradeday, symbol, nowdt, tick, external_datas)

行情tick处理（多实例版本）

一次tick处理，自动分发到所有策略实例

参数:
- tradeday: 交易日
- symbol: 合约/证券代码
- nowdt: (date_int, time_int) 时间元组
- tick: FuturesTick 或 SecurityTick（利用多重派发自动选择实现）
- external_datas: Vector，用户创建的 external_data 实例列表

逻辑流程（与离线回测一致）：
1. 提前更新所有实例的结算价（无论是否生成bar）
2. BarBuilder.feed_tick! （共享，只处理一次）
3. for instance in external_datas:  # 与离线回测的 for 循环一致！
     - sys_data = instance.sys_data  # 直接访问，零开销
     - 更新撑合价
     - on_futures_tick(tradeday, symbol, nowdt, bar, instance)
     - ordertrace_setworstprice!

用户示例:
    # 创建实例
    instances = [
        strategy_param("EMA_FUTURE", "20_10_5", ["SHFE.au"], 20, 10, 5),
        strategy_param("MA_FUTURE", "30_15_3", ["SHFE.ag"], 30, 15, 3)
    ]
    
    # 行情处理（自动根据tick类型派发）
    for tick_msg in market_stream
        tick_data = parse_tick(tick_msg)
        on_md_tick_multi(
            tick_data.tradeday,
            tick_data.symbol,
            tick_data.nowdt,
            tick_data.tick,  # 可以是 FuturesTick 或 SecurityTick
            instances
        )
    end
"""
# 期货版本
function on_md_tick_multi(
    tradeday::Integer,
    nowdt::NTuple{2,Integer},
    ftick::cFuturesTickData,
    external_datas::Vector,
)
    raw_tick = FuturesTick(
        time             = ftick.time,
        status           = ftick.status,
        pre_open_interest = ftick.pre_open_interest,
        pre_close        = ftick.pre_close,
        pre_settle_price = ftick.pre_settle_price,
        open             = ftick.open,
        high             = ftick.high,
        low              = ftick.low,
        match            = ftick.match,
        volume           = ftick.volume,
        turnover         = ftick.turnover,
        open_interest    = ftick.open_interest,
        close            = ftick.close,
        settle_price     = ftick.settle_price,
        high_limited     = ftick.high_limited,
        low_limited      = ftick.low_limited,
        pre_delta        = ftick.pre_delta,
        curr_delta       = ftick.curr_delta,
        ask_price        = ftick.ask_price,
        ask_vol          = ftick.ask_vol,
        bid_price        = ftick.bid_price,
        bid_vol          = ftick.bid_vol,
        trading_status   = UInt8(ftick.trading_status),
    )
    symbol = unsafe_string(pointer(UInt8[ftick.symbol...]))

    # 适配到 FuturesTick 多实例版本
    on_md_tick_multi(tradeday, symbol, nowdt, raw_tick, external_datas)

    return nothing
end

# 期货版本
function on_md_tick_multi(
    tradeday::Integer,
    symbol::String,
    nowdt::NTuple{2,Integer},
    tick::FuturesTick,
    external_datas::Vector
)
    # 1) 提前更新所有实例的结算价（无论是否生成bar）
    if tick.settle_price > 0
        for instance in external_datas
            instance.sys_data.ordertrace_settleprice[symbol] = tick.settle_price
        end
    end
    
    # 2) 从全局获取 BarBuilder
    bb = GLOBAL_BAR_BUILDER[]
    if bb === nothing
        @error "BarBuilder 未初始化，请先调用 init_online_simulation"
        return
    end
    
    # 3) tick → bar 转换（共享的 BarBuilder，只处理一次）
    out = feed_tick!(bb, tradeday, symbol, nowdt, tick)
    if out === nothing
        return  # 当前tick未触发bar结束
    end
    bar, match = out
    
    # 4) 遍历所有策略实例处理bar（与离线回测的 for 循环完全一致）
    for instance in external_datas
        sys_data = instance.sys_data  # ✅ 直接访问，零开销
        
        # 防御性检查：确保交易日已初始化
        if sys_data.ordertrace_tradeday != tradeday
            @warn "等待交易日初始化" strategy=sys_data.strategy_name bar_tradeday=tradeday sys_tradeday=sys_data.ordertrace_tradeday
            continue  # 跳过当前实例，继续处理下一个
        end
        
        # 更新撑合价
        sys_data.last_tick[symbol] = (match.ask_price, match.bid_price)
        
        # 调用策略回调
        try
            global on_futures_tick
            on_futures_tick(tradeday, symbol, nowdt, bar, instance)
            ordertrace_setworstprice!(sys_data, symbol, match.high, match.low)
        catch e
            @error "策略回调执行失败" strategy=sys_data.strategy_name symbol exception=(e, catch_backtrace())
        end
    end
    
    return nothing
end

# 证券版本
function on_md_tick_multi(
    tradeday::Integer,
    nowdt::NTuple{2,Integer},
    stick::cSecurityTickData,
    external_datas::Vector,
)
    raw_tick = SecurityTick(
        time                  = stick.time,
        status                = stick.status,
        pre_close             = stick.pre_close,
        open                  = stick.open,
        high                  = stick.high,
        low                   = stick.low,
        match                 = stick.match,
        ask_price             = stick.ask_price,
        ask_vol               = stick.ask_vol,
        bid_price             = stick.bid_price,
        bid_vol               = stick.bid_vol,
        num_trades            = stick.num_trades,
        volume                = stick.volume,
        turnover              = stick.turnover,
        total_bid_vol         = stick.total_bid_vol,
        total_ask_vol         = stick.total_ask_vol,
        weighted_avg_bid_price = stick.weighted_avg_bid_price,
        weighted_avg_ask_price = stick.weighted_avg_ask_price,
        iopv                  = stick.iopv,
        yield_to_maturity     = stick.yield_to_maturity,
        high_limited          = stick.high_limited,
        low_limited           = stick.low_limited,
        prefix                = stick.prefix,
        syl1                  = stick.syl1,
        syl2                  = stick.syl2,
        sd2                   = stick.sd2,
        trading_phase_code    = stick.trading_phase_code,
        pre_iopv              = stick.pre_iopv,
    )
    symbol = unsafe_string(convert(Ptr{UInt8}, stick.symbol))

    # 适配到 SecurityTick 多实例版本
    on_md_tick_multi(tradeday, symbol, nowdt, raw_tick, external_datas)

    return nothing
end

# 证券版本
function on_md_tick_multi(
    tradeday::Integer,
    symbol::String,
    nowdt::NTuple{2,Integer},
    tick::SecurityTick,
    external_datas::Vector
)
    # 1) 提前更新所有实例的结算价（证券使用成交价）
    if tick.match > 0
        for instance in external_datas
            instance.sys_data.ordertrace_settleprice[symbol] = tick.match
        end
    end
    
    # 2) 从全局获取 BarBuilder
    bb = GLOBAL_BAR_BUILDER[]
    if bb === nothing
        @error "BarBuilder 未初始化，请先调用 init_online_simulation"
        return
    end
    
    # 3) tick → bar 转换（共享的 BarBuilder，只处理一次）
    out = feed_tick!(bb, tradeday, symbol, nowdt, tick)
    if out === nothing
        return  # 当前tick未触发bar结束
    end
    bar, match = out
    
    # 4) 遍历所有策略实例处理bar（与离线回测的 for 循环完全一致）
    for instance in external_datas
        sys_data = instance.sys_data  # ✅ 直接访问，零开销
        
        # 防御性检查：确保交易日已初始化
        if sys_data.ordertrace_tradeday != tradeday
            @warn "等待交易日初始化" strategy=sys_data.strategy_name bar_tradeday=tradeday sys_tradeday=sys_data.ordertrace_tradeday
            continue  # 跳过当前实例，继续处理下一个
        end
        
        # 更新撑合价
        sys_data.last_tick[symbol] = (match.ask_price, match.bid_price)
        
        # 调用策略回调
        try
            global on_futures_tick
            on_futures_tick(tradeday, symbol, nowdt, bar, instance)
            ordertrace_setworstprice!(sys_data, symbol, match.high, match.low)
        catch e
            @error "策略回调执行失败" strategy=sys_data.strategy_name symbol exception=(e, catch_backtrace())
        end
    end
    
    return nothing
end

export on_md_tick_multi

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
        
        # 【改造】使用新的日初始化接口（离线回测版本）
        strategy_on_new_day!(sys_data, date, margin_ratio, price_tick, 
                            multiplier, settleprice, major_codes)
        
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
        
        # 【改造】使用新的日终接口
        strategy_on_day_end!(sys_data, date)
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
