"""Create a self-signed certificate for the LOCAL test broker (not used with ThingsBoard)."""
import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

out = Path(__file__).resolve().parent.parent / "certs"
out.mkdir(exist_ok=True)
key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
now = datetime.datetime.now(datetime.timezone.utc)
cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=3))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"),
                                                    x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False)
        .sign(key, hashes.SHA256()))
(out / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
(out / "key.pem").write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                                                serialization.NoEncryption()))
for name_, extra in (("broker_open.yaml", "  allow-anonymous: true\n  plugins:\n    - auth_anonymous\n"),
                     ("broker_auth.yaml", "  allow-anonymous: false\n  plugins:\n    - auth_file\n  password-file: %s\n" % (out / "passwd"))):
    (out / name_).write_text(
        "listeners:\n  default:\n    type: tcp\n    bind: 127.0.0.1:8883\n    ssl: true\n"
        f"    certfile: {out / 'cert.pem'}\n    keyfile: {out / 'key.pem'}\nsys_interval: 0\nauth:\n{extra}")
(out / "passwd").write_text("someone:$6$salt$notarealhash\n")
print("wrote", out)
