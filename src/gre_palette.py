"""Paleta oficial de cores e ordenação das GREs."""

GRE_ORDER = [str(i) for i in range(1, 17)]
GRE_COLOR_SEQUENCE = [
    "#A8B0B8",  # 1ª
    "#D98B3A",  # 2ª
    "#E6D84A",  # 3ª
    "#73A9D9",  # 4ª
    "#D9BC7A",  # 5ª
    "#B35D5C",  # 6ª
    "#4F89C7",  # 7ª
    "#295D92",  # 8ª
    "#D36A7E",  # 9ª
    "#2F6A7D",  # 10ª
    "#6B6E72",  # 11ª 
    "#E8A7B5",  # 12ª
    "#97C75B",  # 13ª
    "#385D4A",  # 14ª
    "#D8AFC3",  # 15ª
    "#4F9F4F",  # 16ª
]
GRE_COLOR_MAP = dict(zip(GRE_ORDER, GRE_COLOR_SEQUENCE))
GRE_ORDER_MAP = {label: idx for idx, label in enumerate(GRE_ORDER)}
GRE_DISPLAY_LABELS = {
    "1": "1ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "2": "2ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "3": "3ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "4": "4ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "5": "5ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "6": "6ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "7": "7ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "8": "8ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "9": "9ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "10": "10ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "11": "11ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "12": "12ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "13": "13ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "14": "14ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "15": "15ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
    "16": "16ª GERÊNCIA REGIONAL DA EDUCAÇÃO",
}


def ordered_gre_labels(labels) -> list[str]:
    """Retorna os rótulos ordenados segundo o padrão oficial (1-16)."""
    normalized = [str(label) for label in labels if label is not None]
    seen = set()
    ordered = []
    for label in GRE_ORDER:
        if label in normalized and label not in seen:
            ordered.append(label)
            seen.add(label)
    for label in normalized:
        if label not in seen:
            ordered.append(label)
            seen.add(label)
    return ordered


def gre_order_index(label) -> int:
    """Indice numérico usado para ordenação customizada."""
    return GRE_ORDER_MAP.get(str(label), len(GRE_ORDER_MAP))


def gre_display_name(label) -> str:
    return GRE_DISPLAY_LABELS.get(str(label), str(label))


def build_gre_legend_html() -> str:
    """Retorna HTML com a legenda oficial das GREs (3 colunas)."""
    style = """
    <style>
    .siave-gre-legend {display:flex; flex-wrap:wrap; gap:40px; margin-top:12px;}
    .siave-gre-column {display:flex; flex-direction:column; gap:6px; min-width:220px;}
    .siave-gre-item {display:flex; align-items:center; gap:8px; font-size:0.9rem; color:#1f1f1f;}
    .siave-gre-swatch {width:18px; height:18px; border-radius:3px; border:1px solid rgba(0,0,0,0.25);}
    </style>
    """
    groups = [GRE_ORDER[0:5], GRE_ORDER[5:10], GRE_ORDER[10:]]
    columns = []
    for group in groups:
        items = []
        for label in group:
            items.append(
                f"<div class='siave-gre-item'><span class='siave-gre-swatch' "
                f"style='background:{GRE_COLOR_MAP[label]}'></span>{GRE_DISPLAY_LABELS[label]}</div>"
            )
        columns.append("<div class='siave-gre-column'>" + "".join(items) + "</div>")
    return style + "<div class='siave-gre-legend'>" + "".join(columns) + "</div>"
