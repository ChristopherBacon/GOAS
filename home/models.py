


from django.db import models
from wagtail.core.models import Page
from wagtail.core import blocks
from wagtail.core.fields import RichTextField, StreamField
from wagtail.admin.edit_handlers import FieldPanel, StreamFieldPanel
from wagtail.embeds.blocks import EmbedBlock
from wagtail.images.blocks import ImageChooserBlock


class HomePage(Page):
    header = RichTextField(blank=True, null=True)
    body = models.TextField(blank=True, null=True)


    content_panels = Page.content_panels + [
        FieldPanel('header', classname="full"),
        FieldPanel('body', classname="full")
    ]