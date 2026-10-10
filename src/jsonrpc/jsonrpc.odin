// The jsonrpc package: both wire framings (Content-Length headers and
// newline-delimited JSON), the message envelope with ID normalization,
// the Conn that dispatches requests and correlates responses, and the
// bounded outbound writer and request queue.
package jsonrpc
