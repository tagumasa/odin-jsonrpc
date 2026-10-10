// The JSON-RPC message envelope: kinds, normalized IDs, typed error codes,
// and decoding. Batching is rejected (removed from MCP in 2025-06).
package jsonrpc

import "core:encoding/json"
import "core:mem"
import "core:strings"

import "jsonutil:jsonutil"

Msg_Kind :: enum {
	Request,
	Notification,
	Response,
	Error_Response,
}

// Id keeps the JSON type it arrived with: integers become i64, strings
// stay strings (JSON-RPC requires the reply to echo the same value —
// a numeric-looking string id must not collapse into a number).
// Normalization happens exactly once, here.
Id :: union {
	i64,
	string,
}

Err_Code :: enum i32 {
	None              = 0,      // internal "no error" — never a wire code
	Parse_Error       = -32700,
	Invalid_Request   = -32600,
	Method_Not_Found  = -32601,
	Invalid_Params    = -32602,
	Internal_Error    = -32603,
	// The implementation-defined server band: a transient failure a
	// read-only caller may reapply. Callers fold it into their own
	// failure prose — it is never proxied to an MCP client as a wire
	// code.
	Server_Retryable  = -32000,
	Request_Cancelled = -32800, // MCP / LSP reserved
	Request_Failed    = -32803, // LSP 3.17 reserved
}

Envelope :: struct {
	kind:        Msg_Kind,
	id:          Id,
	id_set:      bool,
	method:      string,
	params:      json.Value,
	params_set:  bool,
	result:      json.Value,
	result_set:  bool,
	err_code:    Err_Code,
	err_message: string,
	err_set:     bool,
}

// decode_envelope parses one message body. `a` owns every string and JSON
// value reachable from the returned envelope (free_all releases it).
// env == nil means rejection; code says why (Parse_Error for malformed
// JSON, Invalid_Request for batch envelopes or shape violations). On
// success the code is .None.
decode_envelope :: proc(body: []u8, a: mem.Allocator) -> (env: ^Envelope, code: Err_Code) {
	// Depth/encoding guard first: the core parser would crash on deep
	// nesting and mangle bad UTF-8 silently (see jsonscan.odin).
	if !frame_sanity_ok(body) {
		return nil, .Parse_Error
	}
	value, perr := json.parse_bytes(body, spec = .JSON, parse_integers = true, allocator = a)
	if perr != nil {
		return nil, .Parse_Error
	}
	m, ok := jsonutil.as_object(value)
	if !ok {
		// Top-level array = batch (removed from MCP 2025-06): rejected.
		return nil, .Invalid_Request
	}

	// JSON-RPC 2.0 requires "jsonrpc":"2.0" on every message; every face
	// of this codebase emits it, so demanding it only rejects foreign
	// non-JSON-RPC input early.
	ver, has_ver := m["jsonrpc"]
	if !has_ver {
		return nil, .Invalid_Request
	}
	#partial switch vs in ver {
	case json.String:
		if string(vs) != "2.0" {
			return nil, .Invalid_Request
		}
	case:
		return nil, .Invalid_Request
	}

	env = new(Envelope, a)
	env^ = {}

	id_val, has_id := m["id"]
	if has_id && id_val != nil {
		id, valid := normalize_id(id_val)
		if !valid {
			return nil, .Invalid_Request
		}
		env.id = id
		env.id_set = true
	}

	method_val, has_method := m["method"]
	if has_method {
		#partial switch ms in method_val {
		case json.String:
			env.method = strings.clone(string(ms), a)
		case:
			return nil, .Invalid_Request
		}
		params_val, has_params := m["params"]
		if has_params && params_val != nil {
			env.params = params_val
			env.params_set = true
		}
		env.kind = env.id_set ? .Request : .Notification
		return env, .None
	}

	if !env.id_set {
		return nil, .Invalid_Request
	}

	result_val, has_result := m["result"]
	if has_result {
		env.result = result_val
		env.result_set = true
		env.kind = .Response
		return env, .None
	}

	err_val, has_err := m["error"]
	if has_err {
		env.err_code = .Internal_Error
		env.err_message = ""
		#partial switch ev in err_val {
		case json.Object:
			em := cast(map[string]json.Value)ev
			if cv, found := em["code"]; found {
				#partial switch n in cv {
				case json.Integer:
					env.err_code = code_from_i64(i64(n))
				case:
					env.err_code = .Internal_Error
				}
			}
			if mv, found := em["message"]; found {
				#partial switch ms in mv {
				case json.String:
					env.err_message = strings.clone(string(ms), a)
				case:
				}
			}
		case:
			// A non-object error value is tolerated; the code stays
			// Internal_Error.
		}
		env.kind = .Error_Response
		env.err_set = true
		return env, .None
	}

	return nil, .Invalid_Request
}

code_from_i64 :: proc(v: i64) -> Err_Code {
	switch v {
	case -32700: return .Parse_Error
	case -32600: return .Invalid_Request
	case -32601: return .Method_Not_Found
	case -32602: return .Invalid_Params
	case -32603: return .Internal_Error
	case -32000: return .Server_Retryable
	case -32800: return .Request_Cancelled
	case -32803: return .Request_Failed
	}
	return .Internal_Error
}

// normalize_id keeps the id's JSON type: JSON-RPC requires the reply to
// echo the same value (a numeric-looking string id must stay a string),
// and every internal consumer keys off this same value — request/response
// matching, and the cancelled-notification's requestId lookup.
normalize_id :: proc(v: json.Value) -> (Id, bool) {
	#partial switch x in v {
	case json.Integer:
		return i64(x), true
	case json.String:
		return string(x), true
	}
	return 0, false
}
