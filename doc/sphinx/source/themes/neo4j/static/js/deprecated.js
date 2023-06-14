document.addEventListener('DOMContentLoaded', function () {
    const deprecatedDivs = document.querySelectorAll('div.deprecated')
    deprecatedDivs.forEach(function (deprecatedDiv) {

    // get previous sibling
    const previousSibling = deprecatedDiv.previousElementSibling
    // add deprecated class to previous sibling
    previousSibling.classList.add('deprecated')

    })
  })