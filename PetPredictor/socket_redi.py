from django.http import HttpResponseRedirect
from django.shortcuts import redirect


def some_view(request):
    return redirect("http://google.com")


def out(request):
    return HttpResponseRedirect("http://10.244.1.12:12345/index.html")
