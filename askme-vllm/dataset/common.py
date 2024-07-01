from ..models import Paragraph


def generate_fact_with_context(paragraph: Paragraph):
    if paragraph.subsubsection_name and paragraph.subsection_name:
        context = f"In an article about '{paragraph.page_name}', section '{paragraph.section_name}', subsection '{paragraph.subsection_name}', paragraph '{paragraph.subsubsection_name}'"
    elif paragraph.subsection_name:
        context = f"In an article about '{paragraph.page_name}', section '{paragraph.section_name}', subsection '{paragraph.subsection_name}'"
    else:
        context = f"In an article about '{paragraph.page_name}', section '{paragraph.section_name}'"
    return context, f"{context} mentioned: \n {paragraph.text_cleaned}"
