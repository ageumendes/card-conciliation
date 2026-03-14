import { Global, Module } from '@nestjs/common';
import { AdminAuthController, AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { AdminGuard } from './admin.guard';

@Global()
@Module({
  controllers: [AuthController, AdminAuthController],
  providers: [AuthService, AdminGuard],
  exports: [AuthService, AdminGuard],
})
export class AuthModule {}
