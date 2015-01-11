package vector

import "testing"

func TestPackUnpack(t *testing.T) {

	b, err := ValPack("")
	if err != nil {
		t.Error("valPack fails packing empty string")
	}
	v, err := ValUnpack(b)
	if err != nil {
		t.Error("valPack fails unpacking", err)
	}
	if !v.IsString || v.String != "" {
		t.Error("valPack fails unpacking empty string. Instead got", v.String)
	}

	b, err = ValPack("☢ € → ☎ ❄mung")
	if err != nil {
		t.Error("valPack fails packing string '☢ € → ☎ ❄mung'")
	}
	v, err = ValUnpack(b)
	if err != nil {
		t.Error("valPack fails unpacking", err)
	}
	if !v.IsString || v.String != "☢ € → ☎ ❄mung" {
		t.Error("valPack fails unpacking string '☢ € → ☎ ❄mung'. Instead got", v.String)
	}

	b, err = ValPack(3.25)
	if err != nil {
		t.Error("valPack fails packing 3.25")
	}
	v, err = ValUnpack(b)
	if err != nil {
		t.Error("valPack fails unpacking", err)
	}
	if !v.IsFloat || v.Float != 3.25 {
		t.Error("valPack fails unpacking 3.25. Instead got", v.Float)
	}

	b, err = ValPack(v)
	if err == nil {
		t.Error("expected error for unsupported pack type. Instead got none")
	}
}
