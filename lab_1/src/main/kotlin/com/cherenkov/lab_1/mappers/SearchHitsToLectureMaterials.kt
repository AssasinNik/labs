package com.cherenkov.lab_1.mappers

import com.cherenkov.lab_1.dto.LectureMaterial
import org.springframework.data.elasticsearch.core.SearchHits


fun SearchHits<LectureMaterial>.toList123(): List<LectureMaterial> {
    return this.searchHits.map { it.content }
}