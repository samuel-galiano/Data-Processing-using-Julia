using CSV
using DataFrames
using Statistics

localizacaoArquivo = "dados/tempo.csv"
tabelaTempo = CSV.File(localizacaoArquivo) |> DataFrame

# Filtro das Colunas
filtroAparenciaCeu = combine(groupby(tabelaTempo, :Aparencia), nrow => :Quantidade)
filtroTemperatura = combine(groupby(tabelaTempo, :Temperatura), nrow => :Quantidade)
filtroUmidade = combine(groupby(tabelaTempo, :Umidade), nrow => :Quantidade)
filtroVento = combine(groupby(tabelaTempo, :Vento), nrow => :Quantidade)
filtroJogar = combine(groupby(tabelaTempo, :Jogar), nrow => :Quantidade)

# Tratamento dos Dados
# Tratamento da Coluna de Aparencia do Ceu, filtrando os valores 'menos', e os substituindo para 'sol'
tabelaTempo[tabelaTempo.Aparencia .== "menos", :Aparencia] .= "sol"

# Tratamento da Coluna de Temperatura, filtrando os valores menos de '-130' & maiores de '130', e os substituindo pela mediana da Coluna
tabelaTempo[(tabelaTempo[:, :Temperatura] .< -130) .| (tabelaTempo[:, :Temperatura] .> 130), :Temperatura] .= round(Int64, median(tabelaTempo[!, :Temperatura]))

# Tratamento da Coluna de Umidade, filtrando os valores 'missing', e os substituindo pela mediana da Coluna
# Tratamento da Coluna de Umidade, filtrando os valores menores que '0' & maiores de '100', e os substituíndo pela mediana da Coluna
tabelaTempo[!, :Umidade] .= coalesce.(tabelaTempo.Umidade, round(Int64, median(skipmissing(tabelaTempo.Umidade)))) 
tabelaTempo[(tabelaTempo[:, :Umidade] .< 0) .| (tabelaTempo[:, :Umidade] .> 100), :Umidade] .= round(Int64, median(tabelaTempo[!, :Temperatura]))

# Tratamento da Coluna de Vento, filtrando os valores 'missing', e os substituindo pela maioria.
tabelaTempo[!, :Vento] .= coalesce.(tabelaTempo.Vento, "FALSO") 

filtroAparenciaCeuRecalibrado = combine(groupby(tabelaTempo, :Aparencia), nrow => :Quantidade)
filtroTemperaturaRecalibrado = combine(groupby(tabelaTempo, :Temperatura), nrow => :Quantidade)
filtroUmidadeRecalibrado = combine(groupby(tabelaTempo, :Umidade), nrow => :Quantidade)
filtroVentoRecalibrado = combine(groupby(tabelaTempo, :Vento), nrow => :Quantidade)
filtroJogarRecalibrado = combine(groupby(tabelaTempo, :Jogar), nrow => :Quantidade)

# show(filtroAparenciaCeuRecalibrado)
# show(filtroTemperaturaRecalibrado)
# show(filtroUmidadeRecalibrado)
# show(filtroVentoRecalibrado)
# show(filtroJogarRecalibrado)
show(tabelaTempo)