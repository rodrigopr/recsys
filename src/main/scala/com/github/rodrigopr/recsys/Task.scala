package com.github.rodrigopr.recsys

import com.typesafe.config.Config

trait Task {
  def execute(config: Config): Boolean
}
