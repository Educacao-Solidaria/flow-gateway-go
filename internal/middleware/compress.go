package middleware

import (
	"mime"
	"net/http"
	"strings"

	"github.com/klauspost/compress/gzhttp"
)

// CompressMinSize é o maior corpo que sai sem compressão: o ganho em resposta
// de até 1 KB não paga a CPU, e ela já cabe num pacote.
const CompressMinSize = 1 << 10

// Compress devolve o middleware que comprime as respostas maiores que
// CompressMinSize, escolhendo zstd ou gzip pelo Accept-Encoding do cliente.
//
// A negociação, o buffer até o limiar e os casos de borda são do gzhttp
// (github.com/klauspost/compress):
//   - q-values valem: "gzip;q=0" proíbe gzip; com q iguais, zstd vence;
//   - sem Accept-Encoding, em HEAD e em 204/304 (sem corpo), nada é comprimido;
//   - resposta que já traz Content-Encoding ou Content-Range passa intacta;
//   - comprimindo, tira o Content-Length (o tamanho muda) e o Accept-Ranges;
//   - toda resposta leva "Vary: Accept-Encoding", para cache não servir a
//     versão comprimida a quem não a pediu;
//   - Flush repassa na hora, e o que não vai ser comprimido não espera o limiar.
//
// Só tipos de texto são comprimidos (ver compressible); imagem, áudio, binário
// e arquivo já comprimido passam direto, e SSE também. Um handler que devolva
// segredo junto com eco da entrada (BREACH) desliga a compressão da resposta
// com o header gzhttp.HeaderNoCompression, que não chega ao cliente.
func Compress() func(http.Handler) http.Handler {
	wrap, err := gzhttp.NewWrapper(
		gzhttp.MinSize(CompressMinSize+1), // o gzhttp comprime a partir de MinSize
		gzhttp.ContentTypeFilter(compressible),
	)
	if err != nil {
		panic(err) // configuração fixa: só falha se o código acima estiver errado (coberto por teste)
	}
	return func(next http.Handler) http.Handler { return wrap(next) }
}

// compressible aceita texto, JSON e XML. O gzhttp passa o Content-Type da
// resposta, ou o detectado pelo http.DetectContentType quando ele falta.
//
// ponytail: SSE (text/event-stream) sai sem compressão, evento a evento. Medido
// com o gzhttp: cada stream comprimido segura de 0,6 MB (zstd) a 1 MB (gzip) de
// estado do compressor enquanto fica aberto (1.000 streams ≈ 1 GB) para poupar
// ~34 KB a cada 200 eventos. Se a banda do SSE pesar, comprimir só ele com gzip
// sem estado (gzip.StatelessCompression: 8 KB por stream, ~20% de ganho).
func compressible(contentType string) bool {
	// Só o tipo decide: com parâmetro malformado ele ainda vem, e tipo
	// malformado não casa nenhuma regra.
	mt, _, _ := mime.ParseMediaType(contentType)
	switch mt {
	case "text/event-stream":
		return false
	case "application/json", "application/javascript", "application/xml":
		return true
	}
	return strings.HasPrefix(mt, "text/") || strings.HasSuffix(mt, "+json") || strings.HasSuffix(mt, "+xml")
}
