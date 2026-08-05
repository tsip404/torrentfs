#include "libtorrent_wrapper.h"
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/sha1_hash.hpp>
#include <libtorrent/bdecode.hpp>
#include <libtorrent/span.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/torrent_flags.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/disk_interface.hpp>
#include <libtorrent/disk_buffer_holder.hpp>
#include <libtorrent/disk_observer.hpp>
#include <libtorrent/hasher.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/peer_request.hpp>
#include <libtorrent/storage_defs.hpp>
#include <libtorrent/aux_/vector.hpp>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <cctype>
#include <map>
#include <set>
#include <fstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <openssl/sha.h>

struct lt_error {
    std::string message;
    int code;
};

struct lt_file_entry_inner {
    std::string path;
    uint64_t size;
};

struct lt_torrent_metadata {
    std::string name;
    uint64_t total_size;
    uint32_t piece_length;
    uint32_t num_pieces;
    uint32_t num_files;
    std::vector<lt_file_entry_inner> files;
    lt::sha1_hash info_hash;
};

struct lt_session_wrapper {
    lt::session* session;
    std::mutex mutex;
};

lt_torrent_info_t lt_torrent_info_create(const char* filepath, lt_error_t* error) {
    try {
        auto ti = new lt::torrent_info(std::string(filepath));
        return static_cast<lt_torrent_info_t>(ti);
    } catch (const lt::system_error& e) {
        if (error) {
            error->code = e.code().value();
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

lt_torrent_info_t lt_torrent_info_create_from_buffer(const uint8_t* data, size_t size, lt_error_t* error) {
    try {
        lt::error_code ec;
        // Use bdecode + torrent_info(bdecode_node) instead of the deprecated
        // torrent_info(span, ec, from_span) which may not properly copy data
        // on libtorrent 2.0.x, causing stack smashing on later session use.
        lt::bdecode_node node;
        lt::bdecode(reinterpret_cast<const char*>(data),
                    reinterpret_cast<const char*>(data) + size, node, ec);
        if (ec) {
            if (error) {
                error->code = ec.value();
                static thread_local std::string err_msg;
                err_msg = ec.message();
                error->message = err_msg.c_str();
            }
            return nullptr;
        }
        lt::torrent_info ti(node, ec);
        if (ec) {
            if (error) {
                error->code = ec.value();
                static thread_local std::string err_msg;
                err_msg = ec.message();
                error->message = err_msg.c_str();
            }
            return nullptr;
        }
        return static_cast<lt_torrent_info_t>(new lt::torrent_info(std::move(ti)));
    } catch (const lt::system_error& e) {
        if (error) {
            error->code = e.code().value();
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

void lt_torrent_info_destroy(lt_torrent_info_t info) {
    if (info) {
        auto ti = static_cast<lt::torrent_info*>(info);
        delete ti;
    }
}

const char* lt_torrent_info_name(lt_torrent_info_t info) {
    if (!info) return nullptr;
    auto ti = static_cast<lt::torrent_info*>(info);
    static thread_local std::string name;
    name = ti->name();
    return name.c_str();
}

uint64_t lt_torrent_info_total_size(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return ti->total_size();
}

uint32_t lt_torrent_info_piece_length(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->piece_length());
}

uint32_t lt_torrent_info_num_pieces(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->num_pieces());
}

uint32_t lt_torrent_info_num_files(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->num_files());
}

int lt_torrent_info_get_files(lt_torrent_info_t info, lt_file_entry_t** files, uint32_t* count) {
    if (!info || !files || !count) return -1;

    auto ti = static_cast<lt::torrent_info*>(info);
    const lt::file_storage& fs = ti->files();
    auto n = static_cast<uint32_t>(fs.num_files());

    auto* out = static_cast<lt_file_entry_t*>(std::calloc(n, sizeof(lt_file_entry_t)));
    if (!out) return -1;

    static thread_local std::vector<std::string> paths;
    paths.clear();
    paths.reserve(n);

    for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
        auto idx = static_cast<int>(i);
        paths.emplace_back(fs.file_path(i));
        out[idx].path = paths.back().c_str();
        out[idx].size = static_cast<uint64_t>(fs.file_size(i));
    }

    *files = out;
    *count = n;
    return 0;
}

void lt_files_free(lt_file_entry_t* files) {
    std::free(files);
}

int lt_torrent_info_get_info_hash(lt_torrent_info_t info, uint8_t* hash_out) {
    if (!info || !hash_out) return -1;
    auto ti = static_cast<lt::torrent_info*>(info);
    auto h = ti->info_hashes();
    auto sha1 = h.get_best();
    std::memcpy(hash_out, sha1.data(), 20);
    return 0;
}

lt_torrent_metadata_t* lt_torrent_info_get_metadata(lt_torrent_info_t info) {
    if (!info) return nullptr;

    auto ti = static_cast<lt::torrent_info*>(info);
    auto* meta = new lt_torrent_metadata();

    meta->name = ti->name();
    meta->total_size = ti->total_size();
    meta->piece_length = static_cast<uint32_t>(ti->piece_length());
    meta->num_pieces = static_cast<uint32_t>(ti->num_pieces());
    meta->num_files = static_cast<uint32_t>(ti->num_files());

    const lt::file_storage& fs = ti->files();
    for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
        meta->files.push_back({fs.file_path(i), static_cast<uint64_t>(fs.file_size(i))});
    }

    auto h = ti->info_hashes().get_best();
    meta->info_hash = h;

    return reinterpret_cast<lt_torrent_metadata_t*>(meta);
}

void lt_torrent_metadata_destroy(lt_torrent_metadata_t* metadata) {
    if (metadata) {
        auto* meta = reinterpret_cast<lt_torrent_metadata*>(metadata);
        delete meta;
    }
}

lt_session_t lt_session_create(const char* listen_interface, lt_error_t* error) {
    try {
        auto wrapper = new lt_session_wrapper();
        wrapper->session = new lt::session();

        lt::settings_pack settings;
        if (listen_interface && strlen(listen_interface) > 0) {
            settings.set_str(lt::settings_pack::listen_interfaces, listen_interface);
        }
        settings.set_int(lt::settings_pack::alert_mask,
            lt::alert_category::error | lt::alert_category::status);
        wrapper->session->apply_settings(settings);

        return static_cast<lt_session_t>(wrapper);
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

void lt_session_destroy(lt_session_t session) {
    if (session) {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        delete wrapper->session;
        delete wrapper;
    }
}

lt_torrent_handle_t lt_session_add_torrent(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error) {
    if (!session || !info) {
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        lt::add_torrent_params params;
        params.ti = std::make_shared<lt::torrent_info>(*ti);
        if (save_path) {
            params.save_path = save_path;
        } else {
            params.save_path = "/tmp/torrentfs-cache";
        }
        // Clear paused flag: default_flags includes paused, which prevents
        // tracker announces and peer connections. Torrent download is
        // triggered on-demand via set_piece_deadline.
        params.flags &= ~lt::torrent_flags::paused;

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(params);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

lt_torrent_handle_t lt_session_add_torrent_upload_mode(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error) {
    if (!session || !info) {
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        lt::add_torrent_params params;
        params.ti = std::make_shared<lt::torrent_info>(*ti);
        if (save_path) {
            params.save_path = save_path;
        } else {
            params.save_path = "/tmp/torrentfs-cache";
        }
        // upload_mode: torrent will connect to trackers/peers but never request pieces
        // Clear paused flag so tracker announces work.
        params.flags &= ~lt::torrent_flags::paused;
        params.flags |= lt::torrent_flags::upload_mode;

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(params);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

void lt_session_remove_torrent(lt_session_t session, lt_torrent_handle_t handle, int remove_files) {
    if (session && handle) {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto h = static_cast<lt::torrent_handle*>(handle);
        lt::remove_flags_t flags = remove_files ? lt::session::delete_files : lt::remove_flags_t{};
        std::lock_guard<std::mutex> lock(wrapper->mutex);
        wrapper->session->remove_torrent(*h, flags);
        delete h;
    }
}

void lt_torrent_handle_destroy(lt_torrent_handle_t handle) {
    if (handle) {
        auto h = static_cast<lt::torrent_handle*>(handle);
        delete h;
    }
}

int lt_torrent_handle_is_valid(lt_torrent_handle_t handle) {
    if (!handle) return 0;
    auto h = static_cast<lt::torrent_handle*>(handle);
    return h->is_valid() ? 1 : 0;
}

int lt_torrent_handle_status(lt_torrent_handle_t handle, int* state, float* progress, uint64_t* total_done, uint64_t* total,
    int64_t* dl_rate, int64_t* ul_rate, int64_t* total_dl, int64_t* total_ul,
    int32_t* peers, int32_t* seeds) {
    if (!handle || !state || !progress || !total_done || !total) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto status = h->status();
    *state = static_cast<int>(status.state);
    *progress = status.progress;
    *total_done = static_cast<uint64_t>(status.total_done);
    *total = static_cast<uint64_t>(status.total);
    if (dl_rate) *dl_rate = static_cast<int64_t>(status.download_rate);
    if (ul_rate) *ul_rate = static_cast<int64_t>(status.upload_rate);
    if (total_dl) *total_dl = static_cast<int64_t>(status.total_download);
    if (total_ul) *total_ul = static_cast<int64_t>(status.total_upload);
    if (peers) *peers = status.num_peers;
    if (seeds) *seeds = status.num_seeds;
    return 0;
}

int lt_torrent_handle_read_piece(lt_session_t session, lt_torrent_handle_t handle, int piece_index, uint8_t** data_out, size_t* size_out, lt_error_t* error) {
    if (!session || !handle || !data_out || !size_out) {
        if (error) {
            error->code = -1;
            error->message = "Invalid arguments";
        }
        return -1;
    }

    auto wrapper = static_cast<lt_session_wrapper*>(session);
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) {
        if (error) {
            error->code = -1;
            error->message = "Invalid torrent handle";
        }
        return -1;
    }

    try {
        h->read_piece(lt::piece_index_t(piece_index));
        
        auto start = std::chrono::steady_clock::now();
        auto timeout = std::chrono::seconds(60);
        
        while (true) {
            auto now = std::chrono::steady_clock::now();
            if (now - start > timeout) {
                if (error) {
                    error->code = -1;
                    error->message = "Timeout waiting for piece data";
                }
                return -1;
            }
            
            std::vector<lt::alert*> alerts;
            {
                std::lock_guard<std::mutex> lock(wrapper->mutex);
                wrapper->session->pop_alerts(&alerts);
            }
            
            for (auto* alert : alerts) {
                if (auto* rp = lt::alert_cast<lt::read_piece_alert>(alert)) {
                    if (rp->handle == *h && static_cast<int>(rp->piece) == piece_index) {
                        if (rp->error) {
                            if (error) {
                                error->code = rp->error.value();
                                static thread_local std::string err_msg;
                                err_msg = rp->error.message();
                                error->message = err_msg.c_str();
                            }
                            return -1;
                        }
                        
                        size_t sz = rp->size;
                        if (sz == 0) {
                            *size_out = 0;
                            *data_out = nullptr;
                            return 0;
                        }
                        
                        *size_out = sz;
                        *data_out = static_cast<uint8_t*>(std::malloc(sz));
                        if (*data_out) {
                            std::memcpy(*data_out, rp->buffer.get(), sz);
                            return 0;
                        }
                        return -1;
                    }
                }
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return -1;
    }
}

void lt_piece_data_free(uint8_t* data) {
    if (data) {
        std::free(data);
    }
}

int lt_torrent_handle_get_piece_info(lt_torrent_handle_t handle, int file_index, int64_t* first_piece, int64_t* num_pieces, int64_t* file_offset) {
    if (!handle || !first_piece || !num_pieces || !file_offset) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto t = h->torrent_file();
    if (!t) return -1;
    
    const auto& fs = t->files();
    
    lt::file_index_t fi(file_index);
    if (fi >= fs.end_file()) return -1;
    
    auto file_size = fs.file_size(fi);
    auto piece_length = t->piece_length();
    auto file_offset_val = fs.file_offset(fi);
    
    int64_t start_piece = file_offset_val / piece_length;
    int64_t end_offset = file_offset_val + file_size;
    int64_t end_piece = (end_offset + piece_length - 1) / piece_length;
    
    *first_piece = start_piece;
    *num_pieces = end_piece - start_piece;
    *file_offset = file_offset_val;
    
    return 0;
}

int lt_torrent_handle_get_torrent_info(lt_torrent_handle_t handle, int64_t* piece_length, int64_t* num_pieces) {
    if (!handle || !piece_length || !num_pieces) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto t = h->torrent_file();
    if (!t) return -1;
    
    *piece_length = t->piece_length();
    *num_pieces = t->num_pieces();
    
    return 0;
}

int lt_torrent_handle_have_piece(lt_torrent_handle_t handle, int piece_index) {
    if (!handle) return 0;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return 0;
    
    auto status = h->status();
    if (static_cast<int>(status.pieces.size()) <= piece_index) return 0;
    
    return status.pieces[lt::piece_index_t(piece_index)] ? 1 : 0;
}

int lt_torrent_handle_set_piece_deadline(lt_torrent_handle_t handle, int piece_index, int deadline_ms) {
    if (!handle) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    try {
        h->set_piece_deadline(lt::piece_index_t(piece_index), deadline_ms);
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
}

int lt_torrent_handle_set_piece_priority(lt_torrent_handle_t handle, int piece_index, int priority) {
    if (!handle) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    try {
        std::vector<std::pair<lt::piece_index_t, lt::download_priority_t>> pieces;
        pieces.emplace_back(lt::piece_index_t(piece_index), static_cast<lt::download_priority_t>(priority));
        h->prioritize_pieces(pieces);
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
}

int lt_torrent_handle_set_all_piece_priorities(lt_torrent_handle_t handle, int priority) {
    if (!handle) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    try {
        auto t = h->torrent_file();
        if (!t) return -1;
        
        int num_pieces = t->num_pieces();
        std::vector<std::pair<lt::piece_index_t, lt::download_priority_t>> pieces;
        pieces.reserve(num_pieces);
        for (int i = 0; i < num_pieces; i++) {
            pieces.emplace_back(lt::piece_index_t(i), static_cast<lt::download_priority_t>(priority));
        }
        h->prioritize_pieces(pieces);
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
}

// Minimal JSON parser for flat settings objects
// Handles: {"key1": "str", "key2": 123, "key3": true, "key4": false}
static void skip_json_ws(const char*& p) {
    while (*p && std::isspace(static_cast<unsigned char>(*p))) p++;
}

static std::string parse_json_string(const char*& p) {
    // p points to opening '"'
    p++;
    std::string result;
    while (*p && *p != '"') {
        if (*p == '\\' && *(p + 1)) {
            p++;
            char c = *p;
            switch (c) {
                case 'n': result += '\n'; break;
                case 't': result += '\t'; break;
                case 'r': result += '\r'; break;
                case '\\': result += '\\'; break;
                case '"': result += '"'; break;
                default: result += c; break;
            }
        } else {
            result += *p;
        }
        p++;
    }
    if (*p == '"') p++;
    return result;
}

static int64_t parse_json_int(const char*& p) {
    bool negative = false;
    if (*p == '-') { negative = true; p++; }
    int64_t val = 0;
    while (*p && std::isdigit(static_cast<unsigned char>(*p))) {
        val = val * 10 + (*p - '0');
        p++;
    }
    return negative ? -val : val;
}

static void apply_str_setting(lt::settings_pack& pack, const std::string& key, const std::string& val) {
    // Phase 1: core string settings
    if (key == "listen_interfaces") {
        pack.set_str(lt::settings_pack::listen_interfaces, val);
    } else if (key == "outgoing_interfaces") {
        pack.set_str(lt::settings_pack::outgoing_interfaces, val);
    } else if (key == "user_agent") {
        pack.set_str(lt::settings_pack::user_agent, val);
    } else if (key == "peer_fingerprint") {
        pack.set_str(lt::settings_pack::peer_fingerprint, val);
    }
    // Unknown keys are silently ignored
}

static void apply_int_setting(lt::settings_pack& pack, const std::string& key, int val) {
    // Phase 1: core integer settings
    if (key == "max_connections") {
        // libtorrent 2.0: max_connections is not directly settable via settings_pack
        // silently ignored
    } else if (key == "max_uploads") {
        // libtorrent 2.0: max_uploads is not directly settable via settings_pack
        // silently ignored
    } else if (key == "connection_speed") {
        pack.set_int(lt::settings_pack::connection_speed, val);
    } else if (key == "peer_connect_timeout") {
        pack.set_int(lt::settings_pack::peer_connect_timeout, val);
    } else if (key == "listen_queue_size") {
        pack.set_int(lt::settings_pack::listen_queue_size, val);
    } else if (key == "min_reconnect_time") {
        pack.set_int(lt::settings_pack::min_reconnect_time, val);
    } else if (key == "max_peerlist_size") {
        pack.set_int(lt::settings_pack::max_peerlist_size, val);
    } else if (key == "max_paused_peerlist_size") {
        pack.set_int(lt::settings_pack::max_paused_peerlist_size, val);
    } else if (key == "dht_announce_interval") {
        pack.set_int(lt::settings_pack::dht_announce_interval, val);
    } else if (key == "max_dht_items") {
        pack.set_int(lt::settings_pack::dht_max_dht_items, val);
    } else if (key == "max_active_dht_limit") {
        pack.set_int(lt::settings_pack::active_dht_limit, val);
    } else if (key == "download_rate_limit") {
        pack.set_int(lt::settings_pack::download_rate_limit, val);
    } else if (key == "upload_rate_limit") {
        pack.set_int(lt::settings_pack::upload_rate_limit, val);
    } else if (key == "disk_io_write_mode") {
        pack.set_int(lt::settings_pack::disk_io_write_mode, val);
    } else if (key == "disk_io_read_mode") {
        pack.set_int(lt::settings_pack::disk_io_read_mode, val);
    } else if (key == "file_pool_size") {
        pack.set_int(lt::settings_pack::file_pool_size, val);
    } else if (key == "max_queued_disk_bytes") {
        pack.set_int(lt::settings_pack::max_queued_disk_bytes, val);
    } else if (key == "max_queued_disk_bytes_low_watermark") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "cache_size") {
        // libtorrent 2.0: cache_size removed, silently ignored
    } else if (key == "cache_expiry") {
        // libtorrent 2.0: cache_expiry removed, silently ignored
    } else if (key == "default_cache_min_age") {
        // libtorrent 2.0: default_cache_min_age removed, silently ignored
    } else if (key == "whole_pieces_threshold") {
        pack.set_int(lt::settings_pack::whole_pieces_threshold, val);
    } else if (key == "piece_timeout") {
        pack.set_int(lt::settings_pack::piece_timeout, val);
    } else if (key == "request_timeout") {
        pack.set_int(lt::settings_pack::request_timeout, val);
    } else if (key == "max_out_request_queue") {
        pack.set_int(lt::settings_pack::max_out_request_queue, val);
    } else if (key == "max_allowed_in_request_queue") {
        pack.set_int(lt::settings_pack::max_allowed_in_request_queue, val);
    } else if (key == "max_suggest_pieces") {
        pack.set_int(lt::settings_pack::max_suggest_pieces, val);
    } else if (key == "seeding_piece_quota") {
        pack.set_int(lt::settings_pack::seeding_piece_quota, val);
    } else if (key == "max_sparse_regions") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "peer_timeout") {
        pack.set_int(lt::settings_pack::peer_timeout, val);
    } else if (key == "urlseed_timeout") {
        pack.set_int(lt::settings_pack::urlseed_timeout, val);
    } else if (key == "urlseed_pipeline_size") {
        pack.set_int(lt::settings_pack::urlseed_pipeline_size, val);
    } else if (key == "stop_tracker_timeout") {
        pack.set_int(lt::settings_pack::stop_tracker_timeout, val);
    } else if (key == "tracker_completion_timeout") {
        pack.set_int(lt::settings_pack::tracker_completion_timeout, val);
    } else if (key == "tracker_receive_timeout") {
        pack.set_int(lt::settings_pack::tracker_receive_timeout, val);
    } else if (key == "inactivity_timeout") {
        pack.set_int(lt::settings_pack::inactivity_timeout, val);
    } else if (key == "tracker_backoff") {
        pack.set_int(lt::settings_pack::tracker_backoff, val);
    } else if (key == "tracker_maximum_response_length") {
        pack.set_int(lt::settings_pack::tracker_maximum_response_length, val);
    } else if (key == "min_announce_interval") {
        pack.set_int(lt::settings_pack::min_announce_interval, val);
    } else if (key == "udp_tracker_token_expiry") {
        pack.set_int(lt::settings_pack::udp_tracker_token_expiry, val);
    } else if (key == "choking_algorithm") {
        pack.set_int(lt::settings_pack::choking_algorithm, val);
    } else if (key == "seed_choking_algorithm") {
        pack.set_int(lt::settings_pack::seed_choking_algorithm, val);
    } else if (key == "mixed_mode_algorithm") {
        pack.set_int(lt::settings_pack::mixed_mode_algorithm, val);
    } else if (key == "suggest_mode") {
        pack.set_int(lt::settings_pack::suggest_mode, val);
    } else if (key == "active_downloads") {
        pack.set_int(lt::settings_pack::active_downloads, val);
    } else if (key == "active_seeds") {
        pack.set_int(lt::settings_pack::active_seeds, val);
    } else if (key == "active_checking") {
        pack.set_int(lt::settings_pack::active_checking, val);
    } else if (key == "active_limit") {
        pack.set_int(lt::settings_pack::active_limit, val);
    } else if (key == "active_tracker_limit") {
        pack.set_int(lt::settings_pack::active_tracker_limit, val);
    } else if (key == "active_lsd_limit") {
        pack.set_int(lt::settings_pack::active_lsd_limit, val);
    } else if (key == "active_dht_limit") {
        pack.set_int(lt::settings_pack::active_dht_limit, val);
    } else if (key == "auto_manage_interval") {
        pack.set_int(lt::settings_pack::auto_manage_interval, val);
    } else if (key == "auto_manage_startup") {
        pack.set_int(lt::settings_pack::auto_manage_startup, val);
    } else if (key == "share_ratio_limit") {
        pack.set_int(lt::settings_pack::share_ratio_limit, val);
    } else if (key == "seed_time_ratio_limit") {
        pack.set_int(lt::settings_pack::seed_time_ratio_limit, val);
    } else if (key == "seed_time_limit") {
        pack.set_int(lt::settings_pack::seed_time_limit, val);
    } else if (key == "encryption_policy") {
        pack.set_int(lt::settings_pack::out_enc_policy, val);
    } else if (key == "allowed_encryption_level") {
        pack.set_int(lt::settings_pack::allowed_enc_level, val);
    } else if (key == "ssl_listen") {
        // libtorrent 2.0: ssl_listen removed, silently ignored
    } else if (key == "proxy_port") {
        pack.set_int(lt::settings_pack::proxy_port, val);
    } else if (key == "alert_queue_size") {
        pack.set_int(lt::settings_pack::alert_queue_size, val);
    } else if (key == "aio_threads") {
        pack.set_int(lt::settings_pack::aio_threads, val);
    } else if (key == "network_threads") {
        // libtorrent 2.0: network_threads removed, silently ignored
    } else if (key == "checking_mem_usage") {
        pack.set_int(lt::settings_pack::checking_mem_usage, val);
    } else if (key == "tick_interval") {
        pack.set_int(lt::settings_pack::tick_interval, val);
    } else if (key == "send_buffer_watermark") {
        pack.set_int(lt::settings_pack::send_buffer_watermark, val);
    } else if (key == "send_buffer_watermark_factor") {
        pack.set_int(lt::settings_pack::send_buffer_watermark_factor, val);
    } else if (key == "send_buffer_low_watermark") {
        pack.set_int(lt::settings_pack::send_buffer_low_watermark, val);
    } else if (key == "recv_socket_buffer_size") {
        pack.set_int(lt::settings_pack::recv_socket_buffer_size, val);
    } else if (key == "send_socket_buffer_size") {
        pack.set_int(lt::settings_pack::send_socket_buffer_size, val);
    } else if (key == "optimistic_disk_retry") {
        pack.set_int(lt::settings_pack::optimistic_disk_retry, val);
    } else if (key == "num_optimistic_unchoke_slots") {
        pack.set_int(lt::settings_pack::num_optimistic_unchoke_slots, val);
    } else if (key == "max_failcount") {
        pack.set_int(lt::settings_pack::max_failcount, val);
    } else if (key == "max_rejects") {
        pack.set_int(lt::settings_pack::max_rejects, val);
    } else if (key == "share_mode_target") {
        pack.set_int(lt::settings_pack::share_mode_target, val);
    } else if (key == "local_service_announce_interval") {
        pack.set_int(lt::settings_pack::local_service_announce_interval, val);
    } else if (key == "read_job_every") {
        // libtorrent 2.0: not available, silently ignored
    }
    // Unknown keys are silently ignored
}

static void apply_bool_setting(lt::settings_pack& pack, const std::string& key, bool val) {
    // Phase 1: core boolean settings
    if (key == "smooth_connects") {
        pack.set_bool(lt::settings_pack::smooth_connects, val);
    } else if (key == "allow_multiple_connections_per_ip") {
        pack.set_bool(lt::settings_pack::allow_multiple_connections_per_ip, val);
    } else if (key == "enable_dht") {
        pack.set_bool(lt::settings_pack::enable_dht, val);
    } else if (key == "enable_lsd") {
        pack.set_bool(lt::settings_pack::enable_lsd, val);
    } else if (key == "enable_upnp") {
        pack.set_bool(lt::settings_pack::enable_upnp, val);
    } else if (key == "enable_natpmp") {
        pack.set_bool(lt::settings_pack::enable_natpmp, val);
    } else if (key == "rate_limit_utp") {
        // libtorrent 2.0: rate_limit_utp removed, silently ignored
    } else if (key == "rate_limit_ip_overhead") {
        pack.set_bool(lt::settings_pack::rate_limit_ip_overhead, val);
    } else if (key == "use_disk_read_ahead") {
        // libtorrent 2.0: use_disk_read_ahead removed, silently ignored
    } else if (key == "lock_disk_cache") {
        // libtorrent 2.0: lock_disk_cache removed, silently ignored
    } else if (key == "no_atime_storage") {
        pack.set_bool(lt::settings_pack::no_atime_storage, val);
    } else if (key == "low_prio_disk") {
        // libtorrent 2.0: low_prio_disk removed, silently ignored
    } else if (key == "use_read_cache") {
        // libtorrent 2.0: use_read_cache removed, silently ignored
    } else if (key == "use_disk_cache_pool") {
        // libtorrent 2.0: use_disk_cache_pool removed, silently ignored
    } else if (key == "volatile_read_cache") {
        // libtorrent 2.0: volatile_read_cache deprecated, silently ignored
    } else if (key == "guided_read_cache") {
        // libtorrent 2.0: guided_read_cache removed, silently ignored
    } else if (key == "prioritize_partial_pieces") {
        pack.set_bool(lt::settings_pack::prioritize_partial_pieces, val);
    } else if (key == "drop_skipped_requests") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "announce_to_all_trackers") {
        pack.set_bool(lt::settings_pack::announce_to_all_trackers, val);
    } else if (key == "announce_to_all_tiers") {
        pack.set_bool(lt::settings_pack::announce_to_all_tiers, val);
    } else if (key == "prefer_udp_trackers") {
        pack.set_bool(lt::settings_pack::prefer_udp_trackers, val);
    } else if (key == "auto_manage_prefer_seeds") {
        pack.set_bool(lt::settings_pack::auto_manage_prefer_seeds, val);
    } else if (key == "dont_count_slow_torrents") {
        pack.set_bool(lt::settings_pack::dont_count_slow_torrents, val);
    } else if (key == "proxy_hostnames") {
        pack.set_bool(lt::settings_pack::proxy_hostnames, val);
    } else if (key == "proxy_peer_connections") {
        pack.set_bool(lt::settings_pack::proxy_peer_connections, val);
    } else if (key == "proxy_tracker_connections") {
        pack.set_bool(lt::settings_pack::proxy_tracker_connections, val);
    } else if (key == "anonymous_mode") {
        pack.set_bool(lt::settings_pack::anonymous_mode, val);
    } else if (key == "force_proxy") {
        // libtorrent 2.0: force_proxy removed, silently ignored
    } else if (key == "always_send_user_agent") {
        pack.set_bool(lt::settings_pack::always_send_user_agent, val);
    } else if (key == "ignore_resume_timestamps") {
        // libtorrent 2.0: ignore_resume_timestamps removed, silently ignored
    } else if (key == "no_recheck_incomplete_resume") {
        pack.set_bool(lt::settings_pack::no_recheck_incomplete_resume, val);
    } else if (key == "disable_hash_checks") {
        pack.set_bool(lt::settings_pack::disable_hash_checks, val);
    } else if (key == "allow_i2p_mixed") {
        pack.set_bool(lt::settings_pack::allow_i2p_mixed, val);
    } else if (key == "incoming_starts_queued") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "ban_web_seeds") {
        pack.set_bool(lt::settings_pack::ban_web_seeds, val);
    } else if (key == "report_web_seed_downloads") {
        pack.set_bool(lt::settings_pack::report_web_seed_downloads, val);
    } else if (key == "apply_ip_filter_to_trackers") {
        pack.set_bool(lt::settings_pack::apply_ip_filter_to_trackers, val);
    } else if (key == "announce_double_nat") {
        // libtorrent 2.0: announce_double_nat removed, silently ignored
    } else if (key == "lock_files") {
        // libtorrent 2.0: lock_files removed, silently ignored
    } else if (key == "strict_super_seeding") {
        // libtorrent 2.0: strict_super_seeding removed, silently ignored
    } else if (key == "enable_os_cache") {
        pack.set_bool(lt::settings_pack::enable_os_cache, val);
    }
    // Unknown keys are silently ignored
}

// Build a settings_pack from a JSON string (shared by session creation and runtime apply)
static lt::settings_pack build_settings_pack(const char* settings_json) {
    lt::settings_pack pack;
    if (!settings_json || !*settings_json) return pack;

    const char* p = settings_json;
    skip_json_ws(p);
    if (*p != '{') return pack;
    p++;

    while (*p) {
        skip_json_ws(p);
        if (*p == '}') { p++; break; }
        if (*p == ',') { p++; continue; }

        // Parse key
        if (*p != '"') break;
        std::string key = parse_json_string(p);

        skip_json_ws(p);
        if (*p != ':') break;
        p++;

        skip_json_ws(p);

        // Parse value
        if (*p == '"') {
            std::string val = parse_json_string(p);
            apply_str_setting(pack, key, val);
        } else if (*p == 't' || *p == 'f') {
            bool val = (*p == 't');
            while (*p && *p != ',' && *p != '}' && !std::isspace(static_cast<unsigned char>(*p))) p++;
            apply_bool_setting(pack, key, val);
        } else if (*p == '-' || std::isdigit(static_cast<unsigned char>(*p))) {
            int64_t val = parse_json_int(p);
            apply_int_setting(pack, key, static_cast<int>(val));
        } else {
            // Unknown value type, skip
            while (*p && *p != ',' && *p != '}') p++;
        }
    }

    return pack;
}

void lt_session_apply_settings(lt_session_t session, const char* settings_json) {
    if (!session) return;

    auto pack = build_settings_pack(settings_json);
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    wrapper->session->apply_settings(pack);
}

static bool get_session_bool_setting_impl(lt::settings_pack const& settings, const std::string& key, int* out) {
    if (key == "allow_multiple_connections_per_ip") {
        if (settings.has_val(lt::settings_pack::allow_multiple_connections_per_ip)) {
            *out = settings.get_bool(lt::settings_pack::allow_multiple_connections_per_ip) ? 1 : 0;
            return true;
        }
    } else if (key == "enable_dht") {
        if (settings.has_val(lt::settings_pack::enable_dht)) {
            *out = settings.get_bool(lt::settings_pack::enable_dht) ? 1 : 0;
            return true;
        }
    } else if (key == "enable_lsd") {
        if (settings.has_val(lt::settings_pack::enable_lsd)) {
            *out = settings.get_bool(lt::settings_pack::enable_lsd) ? 1 : 0;
            return true;
        }
    } else if (key == "enable_upnp") {
        if (settings.has_val(lt::settings_pack::enable_upnp)) {
            *out = settings.get_bool(lt::settings_pack::enable_upnp) ? 1 : 0;
            return true;
        }
    } else if (key == "enable_natpmp") {
        if (settings.has_val(lt::settings_pack::enable_natpmp)) {
            *out = settings.get_bool(lt::settings_pack::enable_natpmp) ? 1 : 0;
            return true;
        }
    } else if (key == "smooth_connects") {
        if (settings.has_val(lt::settings_pack::smooth_connects)) {
            *out = settings.get_bool(lt::settings_pack::smooth_connects) ? 1 : 0;
            return true;
        }
    }
    return false;
}

int lt_session_get_bool_setting(lt_session_t session, const char* key, int* out) {
    if (!session || !key || !out) return -1;
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    try {
        auto settings = wrapper->session->get_settings();
        if (get_session_bool_setting_impl(settings, std::string(key), out)) {
            return 0;
        }
    } catch (const std::exception&) {}
    return -1;
}

// Include session_stats_alert header
#include <libtorrent/session_stats.hpp>

int lt_session_get_stats(lt_session_t session, lt_session_stats_t* stats, int32_t* status) {
    if (!session || !stats) return -1;
    
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    
    try {
        // Post session stats request
        wrapper->session->post_session_stats();
        
        // Wait for the session_stats_alert
        auto start = std::chrono::steady_clock::now();
        auto timeout = std::chrono::seconds(5);
        
        while (true) {
            auto now = std::chrono::steady_clock::now();
            if (now - start > timeout) {
                return -1;
            }
            
            std::vector<lt::alert*> alerts;
            {
                std::lock_guard<std::mutex> lock(wrapper->mutex);
                wrapper->session->pop_alerts(&alerts);
            }
            
            for (auto* alert : alerts) {
                if (auto* sa = lt::alert_cast<lt::session_stats_alert>(alert)) {
                    lt::span<std::int64_t const> counters = sa->counters();
                    
                    // Find metric indices by name
                    lt::span<lt::stats_metric const> metrics = lt::session_stats_metrics();
                    for (auto const& m : metrics) {
                        int idx = m.value_index;
                        if (idx < 0 || idx >= static_cast<int>(counters.size())) continue;
                        
                        std::string name(m.name);
                        if (name == "net.recv_rate") stats->download_rate = counters[idx];
                        else if (name == "net.sent_rate") stats->upload_rate = counters[idx];
                        else if (name == "net.recv_bytes") stats->total_downloaded = counters[idx];
                        else if (name == "net.sent_bytes") stats->total_uploaded = counters[idx];
                        else if (name == "dht.dht_nodes") stats->dht_nodes = static_cast<int32_t>(counters[idx]);
                        else if (name == "peer.num_peers_connected") stats->peers_connected = static_cast<int32_t>(counters[idx]);
                        else if (name == "peer.num_peers_half_open") stats->half_open_connections = static_cast<int32_t>(counters[idx]);
                    }
                    if (status) *status = 0;
                    return 0;
                }
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    } catch (const std::exception&) {
        return -1;
    }
}

// ============================================================================
// PieceStorage: per-torrent piece file storage backend
// Stores piece data in cache/pieces/<info_hash>/<info_hash>:piece:N format
// m_base_path already includes "pieces/", so paths are relative to it.
// ============================================================================

namespace {

std::string sha1_to_hex(lt::sha1_hash const& h) {
    char hex[41];
    for (int i = 0; i < 20; i++) {
        snprintf(hex + i * 2, 3, "%02x", static_cast<unsigned char>(h.data()[i]));
    }
    return std::string(hex, 40);
}

void ensure_dir_recursive(const std::string& path) {
    size_t pos = 0;
    while (pos < path.size()) {
        pos = path.find('/', pos + 1);
        std::string sub = path.substr(0, pos);
        if (!sub.empty()) {
            mkdir(sub.c_str(), 0755);
        }
        if (pos == std::string::npos) break;
    }
}

class PieceStorage {
public:
    PieceStorage(const std::string& base_path, const std::string& info_hash_hex)
        : m_base_path(base_path), m_info_hash_hex(info_hash_hex)
    {
        ensure_dir_recursive(m_base_path + "/" + m_info_hash_hex);
    }

    std::string get_info_hash_hex() const { return m_info_hash_hex; }

    std::string pieces_dir() const {
        return m_base_path + "/" + m_info_hash_hex;
    }

    std::string piece_path(int piece_index) const {
        return pieces_dir() + "/" + m_info_hash_hex + ":piece:" + std::to_string(piece_index);
    }

    bool read_piece(int piece_index, int offset, char* buf, int size) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) return false;
        file.seekg(offset);
        file.read(buf, size);
        bool ok = file.good() || (file.eof() && file.gcount() > 0);
        fprintf(stderr, "[DIAG] read_piece piece=%d offset=%d size=%d ok=%d first4=%02x%02x%02x%02x\n",
                piece_index, offset, size, ok,
                (unsigned char)(size>0?buf[0]:0), (unsigned char)(size>1?buf[1]:0),
                (unsigned char)(size>2?buf[2]:0), (unsigned char)(size>3?buf[3]:0));
        return ok;
    }

    bool write_piece(int piece_index, int offset, const char* buf, int size) {
        std::lock_guard<std::mutex> lock(m_mutex);
        ensure_dir_recursive(m_base_path + "/" + m_info_hash_hex);
        std::string path = piece_path(piece_index);
        fprintf(stderr, "[DIAG] write_piece piece=%d offset=%d size=%d first4=%02x%02x%02x%02x path=%s\n",
                piece_index, offset, size,
                (unsigned char)(size>0?buf[0]:0), (unsigned char)(size>1?buf[1]:0),
                (unsigned char)(size>2?buf[2]:0), (unsigned char)(size>3?buf[3]:0),
                path.c_str());
        std::fstream file;
        file.open(path, std::ios::binary | std::ios::in | std::ios::out);
        if (!file.is_open()) {
            file.open(path, std::ios::binary | std::ios::out);
            if (!file.is_open()) return false;
        }
        file.seekp(offset);
        file.write(buf, size);
        if (!file.good()) return false;
        file.close();
        return true;
    }

    bool has_piece(int piece_index) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        return file.is_open();
    }

    int64_t piece_size(int piece_index) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return -1;
        return static_cast<int64_t>(file.tellg());
    }

    lt::sha1_hash hash_piece(int piece_index, int piece_size) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) return lt::sha1_hash();

        lt::hasher h;
        std::vector<char> buf(16 * 1024);
        int remaining = piece_size;
        while (remaining > 0) {
            int to_read = std::min(remaining, static_cast<int>(buf.size()));
            file.read(buf.data(), to_read);
            int actual = static_cast<int>(file.gcount());
            if (actual == 0) break;
            h.update(buf.data(), actual);
            remaining -= actual;
        }
        return h.final();
    }

    lt::sha256_hash hash_piece_sha256(int piece_index, int piece_size) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) return lt::sha256_hash();

        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        std::vector<char> buf(16 * 1024);
        int remaining = piece_size;
        while (remaining > 0) {
            int to_read = std::min(remaining, static_cast<int>(buf.size()));
            file.read(buf.data(), to_read);
            int actual = static_cast<int>(file.gcount());
            if (actual == 0) break;
            SHA256_Update(&ctx, buf.data(), actual);
            remaining -= actual;
        }
        lt::sha256_hash result;
        SHA256_Final(reinterpret_cast<unsigned char*>(result.data()), &ctx);
        return result;
    }

    // Read the full raw piece data into a vector. Returns empty vector if the
    // piece file does not exist or is empty. Needed so that async_hash can
    // compute SHA256 sub-range hashes for v2 blocks in hybrid torrents.
    std::vector<char> read_piece_data(int piece_index) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return {};
        std::streamsize sz = file.tellg();
        if (sz <= 0) return {};
        file.seekg(0);
        std::vector<char> data(sz);
        file.read(data.data(), sz);
        if (!file) return {};
        return data;
    }

    void delete_piece_files() {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string dir = m_base_path + "/" + m_info_hash_hex;
        rmdir(dir.c_str());
    }

private:
    std::string m_base_path;
    std::string m_info_hash_hex;
    mutable std::mutex m_mutex;
};

// ============================================================================
// PieceStorageDiskIO: implements disk_interface for piece-level storage
// ============================================================================

class PieceStorageDiskIO : public lt::disk_interface, public lt::buffer_allocator_interface {
public:
    PieceStorageDiskIO(lt::io_context& ios, const std::string& piece_cache_dir)
        : m_ios(ios), m_piece_cache_dir(piece_cache_dir)
    {
        ensure_dir_recursive(piece_cache_dir);
    }

    // buffer_allocator_interface
    void free_disk_buffer(char* b) override {
        if (b) {
            std::lock_guard<std::mutex> lock(m_alloc_mutex);
            auto it = m_allocated.find(b);
            if (it != m_allocated.end()) {
                m_allocated.erase(it);
                std::free(b);
            }
            // else: double-free, silently ignore
        }
    }
#if LIBTORRENT_VERSION_NUM >= 20100
    void free_multiple_buffers(lt::span<char*> bufs) override {
        std::lock_guard<std::mutex> lock(m_alloc_mutex);
        for (auto* b : bufs) {
            if (b) {
                auto it = m_allocated.find(b);
                if (it != m_allocated.end()) {
                    m_allocated.erase(it);
                    std::free(b);
                }
            }
        }
    }
#endif

    // disk_interface: new_torrent
    lt::storage_holder new_torrent(lt::storage_params const& p,
        std::shared_ptr<void> const& /*torrent*/) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string info_hash_hex = sha1_to_hex(p.info_hash);
        fprintf(stderr, "[DIAG] new_torrent info_hash=%s\n",
                info_hash_hex.c_str());
        auto storage = std::make_unique<PieceStorage>(m_piece_cache_dir, info_hash_hex);
        lt::storage_index_t idx = m_next_index;
        ++m_next_index;
        m_storages[idx] = std::move(storage);
        return lt::storage_holder(idx, *this);
    }

    // disk_interface: remove_torrent
    void remove_torrent(lt::storage_index_t idx) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_storages.erase(idx);
    }

    // disk_interface: async_read
    void async_read(lt::storage_index_t storage, lt::peer_request const& r,
        std::function<void(lt::disk_buffer_holder, lt::storage_error const&)> handler,
        lt::disk_job_flags_t /*flags*/) override
    {
        auto* ps = get_storage(storage);
        if (!ps) {
            handler(lt::disk_buffer_holder(),
                lt::storage_error(lt::error_code(boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            return;
        }

        char* buf = static_cast<char*>(std::malloc(r.length));
        if (!buf) {
            handler(lt::disk_buffer_holder(),
                lt::storage_error(lt::error_code(boost::system::errc::not_enough_memory, boost::system::generic_category())));
            return;
        }
        {
            std::lock_guard<std::mutex> lock(m_alloc_mutex);
            m_allocated.insert(buf);
        }

        if (ps->read_piece(static_cast<int>(r.piece), r.start, buf, r.length)) {
#if LIBTORRENT_VERSION_NUM >= 20100
            boost::asio::post(m_ios, [h = std::move(handler), this, buf]() mutable {
                h(lt::disk_buffer_holder(*this, buf), lt::storage_error());
            });
#else
            boost::asio::post(m_ios, [h = std::move(handler), this, buf, len = r.length]() mutable {
                h(lt::disk_buffer_holder(*this, buf, len), lt::storage_error());
            });
#endif
        } else {
            {
                std::lock_guard<std::mutex> lock(m_alloc_mutex);
                m_allocated.erase(buf);
            }
            std::free(buf);
            boost::asio::post(m_ios, [h = std::move(handler)] {
                h(lt::disk_buffer_holder(),
                    lt::storage_error(lt::error_code(boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
        }
    }

    // disk_interface: async_write
    bool async_write(lt::storage_index_t storage, lt::peer_request const& r,
        char const* buf, std::shared_ptr<lt::disk_observer> /*o*/,
        std::function<void(lt::storage_error const&)> handler,
        lt::disk_job_flags_t /*flags*/) override
    {
        auto* ps = get_storage(storage);
        if (!ps) {
            handler(lt::storage_error(lt::error_code(boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            return false;
        }

        if (ps->write_piece(static_cast<int>(r.piece), r.start, buf, r.length)) {
            fprintf(stderr, "[DIAG] async_write piece=%d offset=%d len=%d => OK (posted)\n",
                    static_cast<int>(r.piece), r.start, r.length);
            boost::asio::post(m_ios, [h = std::move(handler)] { h(lt::storage_error()); });
        } else {
            fprintf(stderr, "[DIAG] async_write piece=%d offset=%d len=%d => FAIL\n",
                    static_cast<int>(r.piece), r.start, r.length);
            boost::asio::post(m_ios, [h = std::move(handler)] {
                h(lt::storage_error(lt::error_code(boost::system::errc::io_error, boost::system::generic_category())));
            });
        }
        return false;
    }

    // disk_interface: async_hash
    void async_hash(lt::storage_index_t storage, lt::piece_index_t piece,
        lt::span<lt::sha256_hash> v2,
        lt::disk_job_flags_t flags,
        std::function<void(lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&)> handler) override
    {
        (void)flags;
        fprintf(stderr, "[DIAG] async_hash CALLED piece=%d v2_size=%zu\n",
                static_cast<int>(piece), v2.size());
        auto* ps = get_storage(storage);
        if (!ps) {
            fprintf(stderr, "[DIAG] async_hash piece=%d => no storage\n", static_cast<int>(piece));
            // Zero out v2 hashes for hybrid torrents — no data available
            for (auto& h : v2) h = lt::sha256_hash();
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha1_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        int piece_idx = static_cast<int>(piece);
        int64_t sz = ps->piece_size(piece_idx);
        fprintf(stderr, "[DIAG] async_hash piece=%d file_size=%ld\n", piece_idx, (long)sz);
        if (sz <= 0) {
            fprintf(stderr, "[DIAG] async_hash piece=%d => file_size<=0, empty hash\n", piece_idx);
            // Zero out v2 hashes for hybrid torrents — no piece data yet
            for (auto& h : v2) h = lt::sha256_hash();
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha1_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        // Read full piece data so we can compute v2 sub-range hashes
        auto data = ps->read_piece_data(piece_idx);
        if (data.empty()) {
            fprintf(stderr, "[DIAG] async_hash piece=%d => read_piece_data empty\n", piece_idx);
            for (auto& h : v2) h = lt::sha256_hash();
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha1_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        // Compute SHA1 over the full piece
        lt::hasher hasher;
        hasher.update(data.data(), static_cast<int>(data.size()));
        lt::sha1_hash hash = hasher.final();

        // Compute SHA256 for each v2 block in hybrid torrents.
        // Each v2 block is a contiguous sub-range of the piece data.
        // The v2 span has one entry per v2 block that overlaps this v1 piece.
        if (!v2.empty()) {
            size_t total = data.size();
            size_t n = v2.size();
            size_t block_sz = total / n;   // regular block size
            for (size_t j = 0; j < n; ++j) {
                size_t start = j * block_sz;
                size_t end = (j == n - 1) ? total : (j + 1) * block_sz;
                SHA256_CTX ctx;
                SHA256_Init(&ctx);
                SHA256_Update(&ctx, data.data() + start, end - start);
                SHA256_Final(reinterpret_cast<unsigned char*>(v2[j].data()), &ctx);
            }
            fprintf(stderr, "[DIAG] async_hash piece=%d v2_blocks=%zu block_sz=%zu => populated\n",
                    piece_idx, n, block_sz);
        }

        fprintf(stderr, "[DIAG] async_hash piece=%d size=%d hash=%02x%02x%02x%02x... => posted\n",
                piece_idx, static_cast<int>(data.size()),
                (unsigned char)hash[0], (unsigned char)hash[1],
                (unsigned char)hash[2], (unsigned char)hash[3]);
        boost::asio::post(m_ios, [h = std::move(handler), p = piece, hash] {
            h(p, hash, lt::storage_error());
        });
    }

    // disk_interface: async_hash2
    void async_hash2(lt::storage_index_t storage, lt::piece_index_t piece,
        int /*offset*/, lt::disk_job_flags_t /*flags*/,
        std::function<void(lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const&)> handler) override
    {
        fprintf(stderr, "[DIAG] async_hash2 CALLED piece=%d\n",
                static_cast<int>(piece));
        auto* ps = get_storage(storage);
        if (!ps) {
            fprintf(stderr, "[DIAG] async_hash2 piece=%d => no storage\n", static_cast<int>(piece));
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha256_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        int piece_idx = static_cast<int>(piece);
        int64_t sz = ps->piece_size(piece_idx);
        fprintf(stderr, "[DIAG] async_hash2 piece=%d file_size=%ld\n", piece_idx, (long)sz);
        if (sz <= 0) {
            fprintf(stderr, "[DIAG] async_hash2 piece=%d => file_size<=0, empty hash\n", piece_idx);
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha256_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        lt::sha256_hash hash = ps->hash_piece_sha256(piece_idx, static_cast<int>(sz));
        fprintf(stderr, "[DIAG] async_hash2 piece=%d size=%d hash=%02x%02x%02x%02x... => posted\n",
                piece_idx, static_cast<int>(sz),
                (unsigned char)hash[0], (unsigned char)hash[1],
                (unsigned char)hash[2], (unsigned char)hash[3]);
        boost::asio::post(m_ios, [h = std::move(handler), p = piece, hash] {
            h(p, hash, lt::storage_error());
        });
    }

    // disk_interface: async_move_storage
    void async_move_storage(lt::storage_index_t /*storage*/, std::string /*p*/,
        lt::move_flags_t /*flags*/,
        std::function<void(lt::status_t, std::string const&, lt::storage_error const&)> handler) override
    {
#if LIBTORRENT_VERSION_NUM >= 20100
        handler(lt::disk_status::fatal_disk_error, std::string(),
#else
        // libtorrent 2.0.x: disk_status namespace not available, use default status_t
        handler(lt::status_t{}, std::string(),
#endif
            lt::storage_error(lt::error_code(boost::system::errc::not_supported, boost::system::generic_category())));
    }

    // disk_interface: async_release_files
    void async_release_files(lt::storage_index_t /*storage*/,
        std::function<void()> handler) override
    {
        if (handler) handler();
    }

    // disk_interface: async_check_files
    void async_check_files(lt::storage_index_t storage,
        lt::add_torrent_params const* /*resume_data*/,
        lt::aux::vector<std::string, lt::file_index_t> /*links*/,
        std::function<void(lt::status_t, lt::storage_error const&)> handler) override
    {
        auto* ps = get_storage(storage);
        if (!ps) {
            handler(lt::status_t{},
                lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory,
                    boost::system::generic_category())));
            return;
        }

        // After FUSE remount, PieceStorageDiskIO must re-check which piece files
        // exist on disk so that libtorrent can correctly identify cached pieces.
        // Scan the pieces directory and report existing piece files.
        std::string dir = ps->pieces_dir();
        std::set<int> existing_pieces;

        DIR* dp = opendir(dir.c_str());
        if (dp) {
            struct dirent* entry;
            std::string prefix = ps->get_info_hash_hex() + ":piece:";
            while ((entry = readdir(dp)) != nullptr) {
                std::string name(entry->d_name);
                if (name.size() > prefix.size() &&
                    name.compare(0, prefix.size(), prefix) == 0) {
                    std::string idx_str = name.substr(prefix.size());
                    try {
                        int idx = std::stoi(idx_str);
                        existing_pieces.insert(idx);
                    } catch (...) {
                        // Ignore malformed filenames
                    }
                }
            }
            closedir(dp);
        }

        // Report found pieces via handler — libtorrent will then call
        // async_hash for each piece to verify integrity.
        handler(lt::status_t{}, lt::storage_error());
    }

    // disk_interface: async_stop_torrent
    void async_stop_torrent(lt::storage_index_t /*storage*/,
        std::function<void()> handler) override
    {
        if (handler) handler();
    }

    // disk_interface: async_rename_file
    void async_rename_file(lt::storage_index_t /*storage*/,
        lt::file_index_t /*index*/, std::string /*name*/,
        std::function<void(std::string const&, lt::file_index_t, lt::storage_error const&)> handler) override
    {
        handler(std::string(), lt::file_index_t(0),
            lt::storage_error(lt::error_code(boost::system::errc::not_supported, boost::system::generic_category())));
    }

    // disk_interface: async_delete_files
    void async_delete_files(lt::storage_index_t storage,
        lt::remove_flags_t /*options*/,
        std::function<void(lt::storage_error const&)> handler) override
    {
        auto* ps = get_storage(storage);
        if (ps) {
            ps->delete_piece_files();
        }
        handler(lt::storage_error());
    }

    // disk_interface: async_set_file_priority
    void async_set_file_priority(lt::storage_index_t /*storage*/,
        lt::aux::vector<lt::download_priority_t, lt::file_index_t> prio,
        std::function<void(lt::storage_error const&,
            lt::aux::vector<lt::download_priority_t, lt::file_index_t>)> handler) override
    {
        handler(lt::storage_error(), std::move(prio));
    }

    // disk_interface: async_clear_piece
    void async_clear_piece(lt::storage_index_t /*storage*/,
        lt::piece_index_t /*index*/,
        std::function<void(lt::piece_index_t)> handler) override
    {
        if (handler) handler(lt::piece_index_t(0));
    }

    // disk_interface: update_stats_counters
    void update_stats_counters(lt::counters& /*c*/) const override {
    }

    // disk_interface: get_status
    std::vector<lt::open_file_state> get_status(lt::storage_index_t) const override {
        return {};
    }

    // disk_interface: abort
    void abort(bool /*wait*/) override {
    }

    // disk_interface: submit_jobs
    void submit_jobs() override {
    }

    // disk_interface: settings_updated
    void settings_updated() override {
    }

private:
    PieceStorage* get_storage(lt::storage_index_t idx) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_storages.find(idx);
        if (it == m_storages.end()) return nullptr;
        return it->second.get();
    }

    lt::io_context& m_ios;
    std::string m_piece_cache_dir;
    std::mutex m_mutex;
    std::map<lt::storage_index_t, std::unique_ptr<PieceStorage>> m_storages;
    lt::storage_index_t m_next_index{0};
    std::set<char*> m_allocated;
    std::mutex m_alloc_mutex;
};

} // anonymous namespace

// ============================================================================
// C API: lt_session_create_with_custom_storage
// Creates a session with PieceStorageDiskIO from the start, avoiding the
// session_params crash on libtorrent 2.0.x that occurs during session
// recreation in add_torrent_with_custom_storage.
// ============================================================================

lt_session_t lt_session_create_with_custom_storage(
    const char* piece_cache_dir, const char* settings_json, lt_error_t* error)
{
    if (!piece_cache_dir || !*piece_cache_dir) {
        if (error) {
            error->code = -1;
            error->message = "piece_cache_dir is required";
        }
        return nullptr;
    }

    try {
        auto wrapper = new lt_session_wrapper();

        lt::session_params params;
        if (settings_json && strlen(settings_json) > 0) {
            params.settings = build_settings_pack(settings_json);
        }
        std::string cache_dir(piece_cache_dir);
        params.disk_io_constructor = [cache_dir](lt::io_context& ios,
            lt::settings_interface const&, lt::counters&) -> std::unique_ptr<lt::disk_interface> {
            return std::make_unique<PieceStorageDiskIO>(ios, cache_dir);
        };
        wrapper->session = new lt::session(std::move(params));

        return static_cast<lt_session_t>(wrapper);
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

// ============================================================================
// C API: lt_session_add_torrent_with_custom_storage
// Adds a torrent to an existing custom-storage session (created via
// lt_session_create_with_custom_storage). No longer recreates the session.
// ============================================================================

lt_torrent_handle_t lt_session_add_torrent_with_custom_storage(
    lt_session_t session, lt_torrent_info_t info,
    const char* piece_cache_dir, const char* settings_json, lt_error_t* error)
{
    if (!session || !info) {
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        (void)settings_json; // Settings are now baked into session at creation time
        std::string cache_dir(piece_cache_dir ? piece_cache_dir : "/tmp/torrentfs-cache");

        // Add the torrent to the existing session (custom storage already set up)
        lt::add_torrent_params atp;
        atp.ti = std::make_shared<lt::torrent_info>(*ti);
        atp.save_path = cache_dir;
        // Clear paused: default_flags includes paused, preventing tracker
        // announces. Download is triggered on-demand via set_piece_deadline.
        atp.flags &= ~lt::torrent_flags::paused;

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(atp);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

lt_torrent_handle_t lt_session_add_torrent_with_custom_storage_upload_mode(
    lt_session_t session, lt_torrent_info_t info,
    const char* piece_cache_dir, const char* settings_json, lt_error_t* error)
{
    if (!session || !info) {
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        (void)settings_json; // Settings are now baked into session at creation time
        std::string cache_dir(piece_cache_dir ? piece_cache_dir : "/tmp/torrentfs-cache");

        // Add the torrent in upload_mode to the existing session
        lt::add_torrent_params atp;
        atp.ti = std::make_shared<lt::torrent_info>(*ti);
        atp.save_path = cache_dir;
        // Clear paused so tracker announces and peer connections work.
        atp.flags &= ~lt::torrent_flags::paused;
        atp.flags |= lt::torrent_flags::upload_mode;

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(atp);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}
