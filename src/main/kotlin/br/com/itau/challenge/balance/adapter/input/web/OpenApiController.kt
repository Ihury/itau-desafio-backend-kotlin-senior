package br.com.itau.challenge.balance.adapter.input.web

import org.springframework.core.io.ClassPathResource
import org.springframework.core.io.Resource
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class OpenApiController {
    @GetMapping("/openapi.yaml")
    fun openApi(): ResponseEntity<Resource> =
        ResponseEntity
            .ok()
            .contentType(APPLICATION_YAML)
            .body(ClassPathResource("static/openapi.yaml"))

    private companion object {
        private val APPLICATION_YAML = MediaType("application", "yaml")
    }
}
