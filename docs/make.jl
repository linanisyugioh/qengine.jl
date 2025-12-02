using qengine
using Documenter

DocMeta.setdocmeta!(qengine, :DocTestSetup, :(using qengine); recursive=true)

makedocs(;
    modules=[qengine],
    authors="linan <linanisyugioh@163.com>",
    sitename="qengine.jl",
    format=Documenter.HTML(;
        canonical="https://linanisyugioh.github.io/qengine.jl",
        edit_link="master",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/linanisyugioh/qengine.jl",
    devbranch="master",
)
