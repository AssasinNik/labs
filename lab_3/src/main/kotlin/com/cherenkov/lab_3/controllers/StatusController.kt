package com.cherenkov.lab_3.controllers

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/")
class StatusController {

    @GetMapping
    fun getStatus(): String {
        return "Everything cool!"
    }
}