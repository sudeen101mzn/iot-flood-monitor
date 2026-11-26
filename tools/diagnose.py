r"""Find out WHY the ThingsBoard connection fails. Run:  python tools/diagnose.py
(optionally a host:  python tools/diagnose.py mqtt.thingsboard.cloud)
Prints one line per step: internet, DNS, plain port 1883, TLS port 8883, certificate check."""
import socket
import ssl
import sys

HOSTS = sys.argv[1:] or ["demo.thingsboard.io", "mqtt.thingsboard.cloud", "mqtt.eu.thingsboard.cloud"]
TIMEOUT = 8


def tcp(host, port):
    try:
        socket.create_connection((host, port), timeout=TIMEOUT).close()
        return "OK", None
    except socket.timeout:
        return "TIMEOUT", "no answer: usually a firewall or the network is blocking this port"
    except ConnectionRefusedError:
        return "REFUSED", "the server answered but nothing listens on this port"
    except OSError as exc:
        return "FAILED", str(exc)


def check(host):
    print(f"\n=== {host} ===")
    try:
        print(f"DNS      OK -> {socket.gethostbyname(host)}")
    except OSError as exc:
        print(f"DNS      FAILED ({exc}): the name cannot be found. Check spelling / try another network or DNS (8.8.8.8)")
        return {}
    res = {}
    for port in (1883, 8883):
        status, why = tcp(host, port)
        res[port] = status
        print(f"port {port} {status}" + (f"  ({why})" if why else ""))
    if res.get(8883) == "OK":
        try:
            with socket.create_connection((host, 8883), timeout=TIMEOUT) as raw, \
                    ssl.create_default_context().wrap_socket(raw, server_hostname=host) as t:
                print(f"TLS      OK ({t.version()}), certificate verified")
                res["tls"] = "OK"
        except ssl.SSLError as exc:
            res["tls"] = "CERT"
            print(f"TLS      FAILED ({exc}): certificate not trusted -> download the CA file from the device's "
                  "'Check connectivity' dialog and run with --ca-file")
        except OSError as exc:
            print(f"TLS      FAILED ({exc})")
    return res


print("=== general internet ===")
net, _ = tcp("www.google.com", 443)
print("port 443 to www.google.com:", net)
if net != "OK":
    print("-> No general internet from Python. Check Wi-Fi, proxy, VPN, or antivirus blocking python.exe.")
summary = {h: check(h) for h in HOSTS}
print("\n=== what this means ===")
for h, r in summary.items():
    if r.get("tls") == "OK":
        print(f"{h}: port 8883 and the certificate work -> use this host (TB_HOST={h}) with TLS.")
    elif r.get(8883) == "OK":
        print(f"{h}: port 8883 is reachable but the certificate is not trusted -> use --ca-file with the CA file from ThingsBoard.")
    elif r.get(1883) == "OK":
        print(f"{h}: only plain port 1883 works. Your network blocks 8883. Try a phone hotspot or another Wi-Fi.")
    elif r:
        print(f"{h}: neither port works from here (blocked or wrong host).")