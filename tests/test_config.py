from pathlib import Path
import yaml

def test_feeds_yaml_parses():
    p = Path("feeds.yml")
    assert p.exists(), "feeds.yml missing"
    cfg = yaml.safe_load(p.read_text())
    assert isinstance(cfg.get("feeds"), list) and cfg["feeds"], "'feeds' list required/non-empty"
    for f in cfg["feeds"]:
        assert "name" in f and "url" in f, "each feed needs name and url"
