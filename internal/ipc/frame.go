// Package ipc implementa o framing do IPC v1 entre o gateway e o
// semantic-cache-rs (docs/ipc-v1.md): cabeçalho de 16 bytes (magic "SCIP",
// versão, kind, flags, request_id e payload_len, em big-endian), o payload
// JSON e um crc32 IEEE sobre tudo o que vem antes dele. O pacote só codifica e
// decodifica frames; o significado de cada kind fica com quem os troca.
package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	// Version é a versão do protocolo falada por este pacote.
	Version = 1
	// HeaderSize é o tamanho do cabeçalho, do magic ao payload_len.
	HeaderSize = 16
	// TrailerSize é o tamanho do crc32 que fecha o frame.
	TrailerSize = 4
	// MaxPayload é o maior payload aceito: 16 MiB.
	MaxPayload = 16 << 20
)

// magic abre todo frame.
const magic = "SCIP"

// Kind identifica o tipo da mensagem.
type Kind uint8

// Kinds do IPC v1.
const (
	KindLookupReq       Kind = 0x01
	KindLookupResp      Kind = 0x02
	KindStoreReq        Kind = 0x03
	KindStoreResp       Kind = 0x04
	KindStatsReq        Kind = 0x05
	KindStatsResp       Kind = 0x06
	KindLookupStreamReq Kind = 0x07
	KindStreamChunk     Kind = 0x08
	KindStreamEnd       Kind = 0x09
	KindPing            Kind = 0x0E
	KindPong            Kind = 0x0F
	KindError           Kind = 0x7F
)

// Códigos do payload de um frame ERROR.
const (
	CodeBadFrame    = "bad_frame"
	CodeBadRequest  = "bad_request"
	CodeTooLarge    = "too_large"
	CodeOverloaded  = "overloaded"
	CodeUnsupported = "unsupported"
	CodeInternal    = "internal"
)

// Frame é uma mensagem do IPC v1. Payload é o JSON da mensagem e pode ser
// vazio; na v1, Flags é sempre 0.
type Frame struct {
	Kind      Kind
	Flags     uint16
	RequestID uint32
	Payload   []byte
}

// FrameError é um frame recusado depois de o cabeçalho ter sido lido. Pelo
// contrato, o receptor responde ERROR com Code, ecoando RequestID, e fecha a
// conexão: não há como ressincronizar o stream. Reason descreve o defeito sem
// nada do payload, que pode ser dado do usuário.
type FrameError struct {
	Code      string // CodeBadFrame ou CodeTooLarge
	RequestID uint32
	Reason    string
}

func (e *FrameError) Error() string { return "ipc: " + e.Code + ": " + e.Reason }

// Append codifica f no fim de dst e devolve o slice estendido. Recusa o que o
// receptor recusaria (flags diferentes de 0, payload acima de MaxPayload) em
// vez de produzir um frame inválido; com erro, dst volta intacto.
func Append(dst []byte, f Frame) ([]byte, error) {
	if f.Flags != 0 {
		return dst, fmt.Errorf("ipc: flags %#04x: a v1 não define nenhum bit", f.Flags)
	}
	// n numa variável: é por ela que o gosec (G115) prova que o uint32(n) abaixo não trunca.
	n := len(f.Payload)
	if n > MaxPayload {
		return dst, fmt.Errorf("ipc: payload de %d bytes acima do teto de %d", n, MaxPayload)
	}
	start := len(dst)
	dst = append(dst, magic...)
	dst = append(dst, Version, byte(f.Kind))
	dst = binary.BigEndian.AppendUint16(dst, f.Flags)
	dst = binary.BigEndian.AppendUint32(dst, f.RequestID)
	dst = binary.BigEndian.AppendUint32(dst, uint32(n))
	dst = append(dst, f.Payload...)
	return binary.BigEndian.AppendUint32(dst, crc32.ChecksumIEEE(dst[start:])), nil
}

// ReadFrame lê exatamente um frame de r, sem consumir nada do seguinte (os
// pedidos podem vir em pipeline na mesma conexão). Fim do stream antes do
// primeiro byte devolve io.EOF; no meio do frame, io.ErrUnexpectedEOF. Magic,
// versão, flags ou crc errados devolvem *FrameError com CodeBadFrame, e
// payload_len acima de MaxPayload, com CodeTooLarge — conferido antes de
// alocar o payload. Kind desconhecido não é erro aqui: o contrato manda
// responder "unsupported" e manter a conexão, e isso é com quem despacha.
func ReadFrame(r io.Reader) (Frame, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	f := Frame{
		Kind:      Kind(hdr[5]),
		Flags:     binary.BigEndian.Uint16(hdr[6:8]),
		RequestID: binary.BigEndian.Uint32(hdr[8:12]),
	}
	reject := func(code, reason string) (Frame, error) {
		return Frame{}, &FrameError{Code: code, RequestID: f.RequestID, Reason: reason}
	}
	n := binary.BigEndian.Uint32(hdr[12:16])
	switch {
	case string(hdr[:4]) != magic:
		return reject(CodeBadFrame, "magic")
	case hdr[4] != Version:
		return reject(CodeBadFrame, fmt.Sprintf("versão %d", hdr[4]))
	case f.Flags != 0:
		return reject(CodeBadFrame, fmt.Sprintf("flags %#04x", f.Flags))
	case n > MaxPayload:
		return reject(CodeTooLarge, fmt.Sprintf("payload_len %d", n))
	}
	// ponytail: o payload é alocado inteiro assim que o cabeçalho passa pelo
	// teto, então um par que declara 16 MiB e não manda segura 16 MiB até o
	// prazo de leitura da conexão. Teto: 16 MiB por conexão aberta. Upgrade, se
	// o lado Go passar a aceitar conexões de terceiros: ler em blocos crescentes.
	buf := make([]byte, n+TrailerSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return Frame{}, err
	}
	crc := crc32.Update(crc32.ChecksumIEEE(hdr[:]), crc32.IEEETable, buf[:n])
	if crc != binary.BigEndian.Uint32(buf[n:]) {
		return reject(CodeBadFrame, "crc")
	}
	f.Payload = buf[:n]
	return f, nil
}
