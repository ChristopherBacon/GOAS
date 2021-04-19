from django.db import models


from wagtail.core.models import Page
from wagtail.core import blocks
from wagtail.core.fields import RichTextField, StreamField
from wagtail.admin.edit_handlers import FieldPanel, StreamFieldPanel
from wagtail.embeds.blocks import EmbedBlock
from wagtail.images.blocks import ImageChooserBlock
from blog.blocks import TwoColumnBlock

class HomePage(Page):
    header = RichTextField(blank=True, null=True)
    body = StreamField([('heading', blocks.CharBlock(from_classname="full title")),
                         ('paragraph', blocks.RichTextBlock()),
                         ('image', ImageChooserBlock(icon="image")),
                         ('two_columns', TwoColumnBlock()),
                         ('embedded_video', EmbedBlock(icon="media"))], null=True, blank=True)

    content_panels = Page.content_panels + [
        FieldPanel('header', classname="full"),
        StreamFieldPanel('body', classname="full")
    ]